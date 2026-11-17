"""HTTP surface tests: the dashboard's API over a real socket."""

import json
import threading
import unittest
import urllib.error
import urllib.request

from skewpipe.server import make_server


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = make_server(port=0, host="127.0.0.1")
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def request(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                return resp.status, raw, ctype
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read(), exc.headers.get("Content-Type", "")

    def json_request(self, method, path, body=None):
        status, raw, _ = self.request(method, path, body)
        return status, json.loads(raw)

    def make_run(self, **settings):
        payload = {"settings": {"seed": 7, "num_aggregators": 4, **settings}}
        status, data = self.json_request("POST", "/api/runs", payload)
        self.assertEqual(status, 201)
        return data["id"]

    def test_meta_lists_metrics_modes_and_strategies(self):
        status, meta = self.json_request("GET", "/api/meta")
        self.assertEqual(status, 200)
        self.assertEqual([m["id"] for m in meta["metrics"]], ["count", "sum", "avg", "distinct"])
        self.assertEqual([m["id"] for m in meta["modes"]], ["naive", "preroute", "split"])
        self.assertEqual(len(meta["strategies"]), 3)
        self.assertEqual(meta["defaults"]["num_aggregators"], 4)

    def test_index_and_assets_are_served(self):
        status, raw, ctype = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", ctype)
        self.assertIn("倾斜".encode(), raw)
        status, _, ctype = self.request("GET", "/app.js")
        self.assertEqual(status, 200)
        self.assertIn("javascript", ctype)
        status, _, _ = self.request("GET", "/../skewpipe/engine.py")
        self.assertEqual(status, 404)

    def test_step_and_finish_drive_the_run_to_exact_results(self):
        run_id = self.make_run(mode="split", metric="avg")
        status, snap = self.json_request("POST", f"/api/runs/{run_id}/step", {"n": 3})
        self.assertEqual(status, 200)
        self.assertEqual(snap["tick"], 3)
        self.assertFalse(snap["finished"])
        status, snap = self.json_request("POST", f"/api/runs/{run_id}/finish", {})
        self.assertEqual(status, 200)
        self.assertTrue(snap["finished"])
        status, res = self.json_request("GET", f"/api/runs/{run_id}/results")
        self.assertEqual(status, 200)
        self.assertEqual(res["metric"], "avg")
        for row in res["rows"]:
            self.assertEqual(row["error"], 0)
        status, base = self.json_request("GET", f"/api/runs/{run_id}/base?key=HOT")
        self.assertEqual(base["base"], "HOT")
        self.assertEqual(base["error"], 0)
        self.assertEqual(len(base["lanes"]), 4)

    def test_actions_are_exposed_over_http(self):
        run_id = self.make_run(mode="split", metric="count")
        self.json_request("POST", f"/api/runs/{run_id}/step", {"n": 8})
        status, out = self.json_request("POST", f"/api/runs/{run_id}/action",
                                        {"action": "kill", "name": "agg-1"})
        self.assertEqual(status, 200)
        self.assertTrue(out["ok"])
        self.assertGreater(out["lost_rows"], 0)
        status, out = self.json_request("POST", f"/api/runs/{run_id}/action",
                                        {"action": "replay-orphaned"})
        self.assertGreater(out["queued"], 0)
        status, snap = self.json_request("POST", f"/api/runs/{run_id}/finish", {})
        self.assertEqual(snap["progress"]["processed"], snap["progress"]["total"])
        status, out = self.json_request("POST", f"/api/runs/{run_id}/action",
                                        {"action": "nonsense"})
        self.assertEqual(status, 400)

    def test_missing_run_is_404(self):
        status, data = self.json_request("GET", "/api/runs/r999")
        self.assertEqual(status, 404)
        self.assertIn("error", data)


if __name__ == "__main__":
    unittest.main()
