"""Tiny dependency-free HTTP server for the pipeline dashboard.

No framework: ``http.server.ThreadingHTTPServer`` + a hand-rolled router.
Everything is in-process (one ``Studio`` holds at most a handful of runs), which
keeps the demo reproducible and the API small.

Run::

    python3 -m skewpipe.server --port 8000
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .engine import JobSettings, Run

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(ROOT, "web")


class Studio:
    """Holds runs by id.  Thread-safe; stepping is serialised per run."""

    def __init__(self) -> None:
        self._runs: Dict[str, Run] = {}
        self._lock = threading.RLock()
        self._seq = 0

    def create(self, settings: dict, autoplay: bool = False) -> dict:
        with self._lock:
            self._seq += 1
            run = Run(JobSettings.from_dict(settings))
            run_id = f"r{self._seq}"
            self._runs[run_id] = run
            if autoplay:
                run.run_to_end()
            return {"id": run_id, "snapshot": run.snapshot()}

    def get(self, run_id: str) -> Run:
        run = self._runs.get(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    def step(self, run_id: str, n: int = 1) -> dict:
        with self._lock:
            run = self.get(run_id)
            for _ in range(max(1, min(2000, n))):
                if run.finished:
                    break
                run.step()
            return run.snapshot()

    def finish(self, run_id: str) -> dict:
        with self._lock:
            run = self.get(run_id)
            run.run_to_end()
            return run.snapshot()

    def action(self, run_id: str, action: str, body: dict) -> Any:
        with self._lock:
            run = self.get(run_id)
            return do_action(run, action, body)


def do_action(run: Run, action: str, body: dict) -> Any:
    body = body or {}
    if action == "split":
        key = body.get("key") or run.settings.hot_key
        return {"ok": True, "split": run.start_split(
            key, body.get("strategy"), body.get("fanout"), reason="manual")}
    if action == "retrigger":
        key = body.get("key") or run.settings.hot_key
        return run.retrigger(key, body.get("strategy"), body.get("fanout"))
    if action == "kill":
        return run.kill(body.get("name") or "agg-0")
    if action == "kill-all":
        return run.kill_all()
    if action == "kill-publisher":
        return run.kill_publisher()
    if action == "suspend":
        return run.suspend(body.get("name") or "agg-0", bool(body.get("on", True)))
    if action == "duplicate":
        return run.duplicate_publish(body.get("name") or "")
    if action == "replay-old":
        return run.replay_old_incarnation()
    if action == "replay-stale-split":
        return run.replay_stale_split()
    if action == "replay-orphaned":
        return run.replay_orphaned()
    raise ValueError(f"unknown action {action!r}")


STATIC_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
    ".json": "application/json; charset=utf-8",
}


class Handler(BaseHTTPRequestHandler):
    studio: Studio

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet
        if os.environ.get("SKEWPIPE_HTTP_LOG"):
            super().log_message(fmt, *args)

    # -- helpers -----------------------------------------------------------
    def _send(self, status: int, payload: Any, ctype: str) -> None:
        body = payload if isinstance(payload, bytes) else json.dumps(
            payload, ensure_ascii=False, default=_json_default).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, payload: Any, status: int = 200) -> None:
        self._send(status, payload, "application/json; charset=utf-8")

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode() or "{}")
        except (ValueError, UnicodeDecodeError):
            return {}

    def _static(self, path: str) -> None:
        rel = path.lstrip("/") or "index.html"
        full = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not full.startswith(STATIC_DIR) or not os.path.isfile(full):
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        ctype = STATIC_TYPES.get(os.path.splitext(full)[1], "application/octet-stream")
        with open(full, "rb") as fh:
            self._send(200, fh.read(), ctype)

    # -- routing -----------------------------------------------------------
    def do_GET(self) -> None:
        url = urlparse(self.path)
        try:
            if url.path.startswith("/api/"):
                self._api_get(url.path, parse_qs(url.query))
            else:
                self._static(url.path)
        except KeyError:
            self._json({"error": "run not found"}, 404)
        except Exception:  # pragma: no cover - surface bugs as JSON
            traceback.print_exc()
            self._json({"error": "internal error"}, 500)

    def do_POST(self) -> None:
        url = urlparse(self.path)
        try:
            if url.path.startswith("/api/"):
                self._api_post(url.path, self._body())
            else:
                self._json({"error": "not found"}, 404)
        except KeyError:
            self._json({"error": "run not found"}, 404)
        except ValueError as exc:
            self._json({"error": str(exc)}, 400)
        except Exception:  # pragma: no cover
            traceback.print_exc()
            self._json({"error": "internal error"}, 500)

    def _api_get(self, path: str, query: Dict[str, list]) -> None:
        parts = [p for p in path.split("/") if p]
        if path == "/api/health":
            return self._json({"ok": True})
        if path == "/api/meta":
            from . import combine
            from .engine import JobSettings as JS
            defaults = JS()
            return self._json({
                "metrics": [
                    {"id": m, **combine.METRIC_INFO[m]} for m in combine.METRIC_ORDER
                ],
                "modes": [
                    {"id": "naive", "label": "朴素哈希分区",
                     "detail": "按 hash(key) 分区，热点整键压在一个进程"},
                    {"id": "preroute", "label": "入口预路由",
                     "detail": "入口即改写键名；代价是必须从开始就决定"},
                    {"id": "split", "label": "运行时拆分重键",
                     "detail": "运行中检测倾斜→拆分任务→搬运旧证据→按 lane 合并"},
                ],
                "strategies": [
                    {"id": "round_robin", "label": "轮转 round-robin"},
                    {"id": "striped", "label": "条带 striped"},
                    {"id": "keyed_lane", "label": "分组键 keyed"},
                ],
                "defaults": vars(defaults),
            })
        if len(parts) == 3 and parts[1] == "runs":
            return self._json(self.studio.get(parts[2]).snapshot())
        if len(parts) == 4 and parts[1] == "runs":
            run = self.studio.get(parts[2])
            if parts[3] == "results":
                return self._json(run.results())
            if parts[3] == "base":
                key = (query.get("key") or [None])[0]
                return self._json(run.base_view(key))
            if parts[3] == "snapshot":
                return self._json(run.snapshot())
            if parts[3] == "settings":
                return self._json(vars(run.settings))
        self._json({"error": "not found"}, 404)

    def _api_post(self, path: str, body: dict) -> None:
        if path == "/api/runs":
            created = self.studio.create(body.get("settings") or body, bool(body.get("autoplay")))
            return self._json(created, 201)
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 4 and parts[1] == "runs":
            run_id, verb = parts[2], parts[3]
            if verb == "step":
                return self._json(self.studio.step(run_id, int(body.get("n", 1))))
            if verb == "finish":
                return self._json(self.studio.finish(run_id))
            if verb == "action":
                return self._json(self.studio.action(run_id, body.get("action", ""), body))
            if verb == "results":
                import dataclasses
                return self._json(self.studio.get(run_id).results())
        self._json({"error": "not found"}, 404)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, set):
        return sorted(obj)[:200]
    if dataclasses_is_dataclass(obj):
        import dataclasses
        return dataclasses.asdict(obj)
    return str(obj)


def dataclasses_is_dataclass(obj: Any) -> bool:
    import dataclasses
    return dataclasses.is_dataclass(obj)


def make_server(port: int = 8000, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    studio = Studio()
    handler = type("BoundHandler", (Handler,), {"studio": studio})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    server = make_server(args.port, args.host)
    print(f"skewpipe dashboard: http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
