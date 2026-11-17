"""Headless demo: run the pipeline and print the verification table.

This is the same engine the dashboard drives, so anything shown here can be
reproduced in the browser::

    python3 -m skewpipe.demo                 # all modes × metrics
    python3 -m skewpipe.demo --metric avg --mode split
    python3 -m skewpipe.demo --failures      # exercise exit / dup / stale split

Exit status is non-zero if any result differs from the ground truth.
"""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, List

from . import combine
from .engine import JobSettings, Run

MODES = ("naive", "preroute", "split")


def scenario(mode: str, metric: str, args) -> dict:
    run = Run(JobSettings.from_dict({
        "mode": mode, "metric": metric, "strategy": args.strategy,
        "num_aggregators": args.aggregators, "fanout": args.fanout, "seed": args.seed,
    }))
    run.run_to_end()
    rows = run.results()["rows"]
    hot = next(r for r in rows if r["base"] == run.settings.hot_key)
    errors = [abs(r["error"]) for r in rows if r["error"] is not None]
    return {
        "run": run,
        "mode": mode,
        "metric": metric,
        "hot": hot,
        "bad": max(errors) if errors else None,
        "peak_queue": max((h["max_queue"] for h in run.history), default=0),
        "ticks": run.final_tick or run.tick,
        "converged": sum(r["rows"] for r in rows) == run.source.row_count,
    }


def fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    return f"{v:,}" if isinstance(v, int) else str(v)


def print_table(results: List[dict]) -> None:
    head = ("模式", "指标", "热点结果", "真值", "最大误差", "峰值积压", "tick", "行数守恒")
    rows = [
        (r["mode"], r["metric"], fmt(r["hot"]["value"]), fmt(r["hot"]["truth"]),
         fmt(r["bad"]), f"{r['peak_queue']:,}", str(r["ticks"]), "是" if r["converged"] else "否")
        for r in results
    ]
    widths = [max(len(str(x)) for x in col) for col in zip(head, *rows)]
    line = "  ".join
    print(line(str(c).ljust(w) for c, w in zip(head, widths)))
    print(line("-" * w for w in widths))
    for row in rows:
        print(line(str(c).ljust(w) for c, w in zip(row, widths)))


def print_detail(result: dict) -> None:
    run, metric = result["run"], result["metric"]
    hot = result["hot"]
    info = combine.METRIC_INFO[metric]
    print(f"\n{result['mode']} · {metric} · {info['merge']} · 证据: {info['evidence']}")
    print(f"  热点键 {hot['base']}: 合并结果 {fmt(hot['value'])} · 真值 {fmt(hot['truth'])} "
          f"· 误差 {fmt(hot['error'])} · {hot['rows']:,} 行")
    for lane in hot["lanes"]:
        print(f"    {lane['identity']:<10} {fmt(lane['value']):>12}  ({lane['rows']:,} 行)")
    if hot.get("split_id"):
        print(f"  拆分任务: {hot['split_id']}")
    view = run.base_view(hot["base"])
    naive = view.get("naive")
    if naive:
        print(f"  朴素相加: {fmt(naive['value'])}（{naive['rule']}）· 偏差 {fmt(naive['error'])}")


def failures(args) -> int:
    """Exercise every rejection path and confirm the result stays exact."""

    ok = True
    for metric in combine.METRIC_ORDER:
        run = Run(JobSettings.from_dict({
            "mode": "split", "metric": metric, "strategy": args.strategy,
            "num_aggregators": args.aggregators, "fanout": args.fanout, "seed": args.seed,
        }))
        # advance to a tick where agg-0 holds unpublished evidence and has
        # already published something, so the exit is lossy *and* duplicate
        # reporting has live partials to replay
        while ((not any(d["rows"] for d in run.aggs[0].delta.values())
                and not run.aggs[0].queue) or not run.aggs[0].receipts) and run.tick < 400:
            run.step()
        dup = run.duplicate_publish()
        kill = run.kill("agg-0")
        for _ in range(2):
            run.step()
        replayed = run.replay_orphaned()
        run.retrigger("HOT", args.strategy)
        stale = run.replay_stale_split()
        old = run.replay_old_incarnation()
        run.run_to_end()
        rows = run.results()["rows"]
        bad = [r for r in rows if r["error"] not in (None, 0)]
        reply = run.snapshot()["publisher"]["reject_reasons"]
        status = "OK" if not bad else "FAIL"
        ok = ok and not bad
        print(f"{metric:<9} 退出丢失 {kill['lost_rows']:>4} 行 · 重放 {replayed['queued']:>4} · "
              f"重复 {dup['queued']:>3} · 旧拆分 {stale['queued']:>3} · 旧代 {old['queued']:>3} · "
              f"拒绝 {reply} · 误差 {max((abs(r['error']) for r in rows), default=0)}  {status}")
    return 0 if ok else 1


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", default="all", choices=(*MODES, "all"))
    parser.add_argument("--metric", default="all", choices=(*combine.METRIC_ORDER, "all"))
    parser.add_argument("--strategy", default="round_robin",
                        choices=("round_robin", "striped", "keyed_lane"))
    parser.add_argument("--aggregators", type=int, default=4)
    parser.add_argument("--fanout", type=int, default=4)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--detail", action="store_true", help="print per-lane contributions")
    parser.add_argument("--failures", action="store_true",
                        help="exercise exits, duplicate reports and stale split replays")
    parser.add_argument("--strict", action="store_true",
                        help="exit non-zero when any result differs from ground truth")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.failures:
        return failures(args)

    modes = MODES if args.mode == "all" else (args.mode,)
    metrics = combine.METRIC_ORDER if args.metric == "all" else (args.metric,)
    results = [scenario(mode, metric, args) for mode in modes for metric in metrics]
    print_table(results)
    if args.detail or (args.mode != "all" and args.metric != "all"):
        print_detail(results[0])
    worst = max((r["bad"] for r in results if r["bad"] is not None), default=0)
    print(f"\n所有结果与真值最大误差: {fmt(worst)} · 组合数 {len(results)}")
    if args.strict and worst:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
