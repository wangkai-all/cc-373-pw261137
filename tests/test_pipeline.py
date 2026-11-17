"""End-to-end pipeline tests: exactness, split contract, failure, determinism."""

import unittest

from skewpipe import JobSettings, Run
from skewpipe import combine
from skewpipe.engine import lane_for


def make(**kw):
    base = {"seed": 7, "num_aggregators": 4, "fanout": 4}
    base.update(kw)
    return Run(JobSettings.from_dict(base))


def run_to_end(run, max_ticks=4000):
    run.run_to_end(max_ticks)
    return run


def key_errors(run):
    """(base, error) for every result row; truth is known for every base."""
    return {row["base"]: row["error"] for row in run.results()["rows"]}


def advance_until_lossy(run, name="agg-0", limit=200):
    """Step until the named process holds evidence that has not been published."""

    for _ in range(limit):
        agg = next(a for a in run.aggs if a.name == name)
        if agg.queue or any(d["rows"] for d in agg.delta.values()):
            return True
        run.step()
    return False


class ExactnessTests(unittest.TestCase):
    """Every mode × metric must reproduce the ground truth exactly."""

    def test_all_modes_and_metrics_match_ground_truth(self):
        for mode in ("naive", "preroute", "split"):
            for metric in combine.METRIC_ORDER:
                with self.subTest(mode=mode, metric=metric):
                    run = run_to_end(make(mode=mode, metric=metric))
                    rows = run.results()["rows"]
                    self.assertTrue(rows)
                    for row in rows:
                        self.assertEqual(row["error"], 0,
                                         f"{mode}/{metric} {row['base']} → {row['value']} != {row['truth']}")
                    self.assertEqual(sum(r["rows"] for r in rows), run.source.row_count)

    def test_split_actually_relieves_the_hot_process(self):
        naive = run_to_end(make(mode="naive", metric="count"))
        split = run_to_end(make(mode="split", metric="count"))

        naive_peak = max(h["max_queue"] for h in naive.history)
        split_peak = max(h["max_queue"] for h in split.history)
        self.assertGreater(naive_peak, 1500, "naive should pile up the hot key")
        self.assertLess(split_peak, naive_peak / 4, "split should drain the backlog")

        # the hot key must be spread over all four lanes in the split run
        hot = next(r for r in split.results()["rows"] if r["base"] == split.settings.hot_key)
        self.assertEqual(len(hot["lanes"]), 4)
        self.assertEqual(hot["split_id"], "split-01")
        self.assertGreater(hot["lanes"][0]["rows"], 0)

    def test_split_keeps_row_conservation(self):
        run = run_to_end(make(mode="split", metric="distinct"))
        published = sum(st["rows"] for st in run.publisher.state.values())
        self.assertEqual(published, run.source.row_count)
        self.assertEqual(run.snapshot()["progress"]["processed"], run.source.row_count)


class SplitContractTests(unittest.TestCase):
    def test_split_task_lane_digest_binds_the_lane_set(self):
        run = make(mode="split", metric="count")
        for _ in range(6):
            run.step()
        snap = run.start_split("HOT", "round_robin", 4, reason="manual")
        self.assertEqual(snap["lanes"], ["HOT#p0", "HOT#p1", "HOT#p2", "HOT#p3"])
        self.assertEqual(len(snap["lane_digest"]), 12)
        other = run.start_split("HOT", "striped", 6, reason="retrigger")
        self.assertNotEqual(snap["lane_digest"], other["lane_digest"])
        self.assertEqual(other["epoch"], snap["epoch"] + 1)
        retired = [t for t in run.snapshot()["splits"]["tasks"] if t["split_id"] == "split-01"]
        self.assertTrue(retired and retired[0]["retired"])

    def test_old_split_partials_are_rejected_and_results_stay_exact(self):
        run = make(mode="split", metric="count")
        for _ in range(10):
            run.step()
        run.retrigger("HOT", "striped")       # retire split-01
        queued = run.replay_stale_split()["queued"]
        self.assertGreater(queued, 0)
        run_to_end(run)
        reasons = run.snapshot()["publisher"]["reject_reasons"]
        self.assertGreaterEqual(reasons.get("retired-split", 0), queued)
        self.assertEqual(set(key_errors(run).values()), {0})

    def test_stale_split_does_not_pollute_a_higher_epoch_result(self):
        run = make(mode="split", metric="sum")
        run.run_to_end()
        before = {r["base"]: r["value"] for r in run.results()["rows"]}
        # craft a partial under a retired task id, bound to a live lane name
        live = run.tasks.active_for("HOT")
        run.publisher.queue.append({
            "event_id": "forged-stale", "metric": "sum", "identity": "HOT#p0",
            "base": "HOT", "inc": run.inc, "code": run.code,
            "split_id": "split-00-not-real", "lane": 0, "rows": 999,
            "evidence": {}, "delta": {"rows": 999, "sum": 999},
        })
        run.publish()
        self.assertEqual(run.publisher.rejected[-1]["reason"], "unknown-split")
        run.publisher.queue.append({
            "event_id": "forged-lane", "metric": "sum", "identity": "HOT#p99",
            "base": "HOT", "inc": run.inc, "code": run.code,
            "split_id": live.split_id, "lane": 99, "rows": 5,
            "evidence": {}, "delta": {"rows": 5, "sum": 5},
        })
        run.publish()
        self.assertEqual(run.publisher.rejected[-1]["reason"], "lane-mismatch")
        after = {r["base"]: r["value"] for r in run.results()["rows"]}
        self.assertEqual(before, after)


class PublisherContractTests(unittest.TestCase):
    def test_duplicate_partials_are_deduped_by_event_id(self):
        run = make(mode="preroute", metric="avg")
        for _ in range(8):
            run.step()
        out = run.duplicate_publish()
        self.assertGreater(out["queued"], 0)
        run.step()
        run_to_end(run)
        reasons = run.snapshot()["publisher"]["reject_reasons"]
        self.assertGreaterEqual(reasons.get("duplicate-publish", 0), out["queued"])
        self.assertEqual(set(key_errors(run).values()), {0})

    def test_foreign_incarnation_and_stale_code_are_rejected(self):
        run = make(mode="naive", metric="count")
        run.run_to_end()
        before = {r["base"]: r["value"] for r in run.results()["rows"]}
        out = run.replay_old_incarnation()
        self.assertGreater(out["queued"], 0)
        run.publish()
        reasons = [r["reason"] for r in run.publisher.rejected]
        self.assertIn("foreign-incarnation", reasons)
        after = {r["base"]: r["value"] for r in run.results()["rows"]}
        self.assertEqual(before, after)

    def test_publisher_restart_rebuilds_exactly_from_aggregate_state(self):
        for metric in combine.METRIC_ORDER:
            with self.subTest(metric=metric):
                run = make(mode="split", metric=metric)
                for _ in range(20):
                    run.step()
                live_before = run.publisher.state_entries()
                run.kill_publisher()
                run.step()  # the rebuild happens at the top of the tick
                self.assertGreater(run.publisher.state_entries(), 0)
                run_to_end(run)
                self.assertEqual(set(key_errors(run).values()), {0},
                                 f"{metric} diverged after publisher rebuild")
                self.assertGreater(live_before, 0)


class FailureRecoveryTests(unittest.TestCase):
    def test_worker_exit_loses_evidence_and_replay_restores_exactness(self):
        for metric in combine.METRIC_ORDER:
            with self.subTest(metric=metric):
                run = make(mode="split", metric=metric)
                self.assertTrue(advance_until_lossy(run), "no unpublished evidence to lose")
                out = run.kill("agg-0")
                self.assertTrue(out["ok"])
                self.assertGreater(out["lost_rows"], 0, "kill should lose unpublished rows")
                for _ in range(3):
                    run.step()
                run.replay_orphaned()
                run_to_end(run)
                self.assertEqual(set(key_errors(run).values()), {0})
                self.assertEqual(run.snapshot()["progress"]["processed"], run.source.row_count)

    def test_without_replay_the_loss_is_visible_and_real(self):
        run = make(mode="split", metric="count")
        self.assertTrue(advance_until_lossy(run))
        out = run.kill("agg-0")
        self.assertGreater(out["lost_rows"], 0)
        run_to_end(run)
        rows = run.results()["rows"]
        hot = next(r for r in rows if r["base"] == run.settings.hot_key)
        # rows folded but never published are genuinely missing without a replay
        self.assertLess(run.snapshot()["progress"]["processed"], run.source.row_count)
        self.assertNotEqual(hot["error"], 0)

    def test_rejudged_partials_do_not_inflate_progress(self):
        run = make(mode="split", metric="count")
        for _ in range(12):
            run.step()
        before = run.snapshot()["progress"]["processed"]
        self.assertGreater(before, 0)
        run.duplicate_publish()
        run.replay_old_incarnation()
        self.assertEqual(run.snapshot()["progress"]["processed"], before,
                         "queued replays must not count as newly processed rows")
        run_to_end(run)
        self.assertEqual(run.snapshot()["progress"]["processed"], run.source.row_count)

    def test_suspend_builds_backlog_and_resume_drains_it(self):
        run = make(mode="naive", metric="count")
        run.suspend("agg-0", True)
        for _ in range(8):
            run.step()
        while not run.finished and run.aggs[0].queue and len(run.aggs[0].queue) < 50:
            run.step()
        self.assertTrue(run.aggs[0].queue, "suspended process must accumulate a backlog")
        run.suspend("agg-0", False)
        run_to_end(run)
        self.assertEqual(set(key_errors(run).values()), {0})


class DeterminismTests(unittest.TestCase):
    def test_same_settings_same_run(self):
        a = run_to_end(make(mode="split", metric="distinct"))
        b = run_to_end(make(mode="split", metric="distinct"))
        self.assertEqual(a.final_tick, b.final_tick)
        self.assertEqual(a.results(), b.results())
        self.assertEqual(
            [(h["tick"], h["max_queue"], h["processed"]) for h in a.history],
            [(h["tick"], h["max_queue"], h["processed"]) for h in b.history],
        )

    def test_different_seed_changes_the_stream_but_not_the_truth_mapping(self):
        a = run_to_end(make(mode="naive", metric="sum", seed=7))
        b = run_to_end(make(mode="naive", metric="sum", seed=8))
        va = {r["base"]: r["value"] for r in a.results()["rows"]}
        vb = {r["base"]: r["value"] for r in b.results()["rows"]}
        self.assertNotEqual(va, vb)
        for run in (a, b):
            self.assertEqual(set(key_errors(run).values()), {0})


class LaneRuleTests(unittest.TestCase):
    def test_strategies_are_deterministic_and_spread_the_hot_key(self):
        for strategy in ("round_robin", "striped", "keyed_lane"):
            lanes = [lane_for(strategy, rid, 4) for rid in range(4000)]
            self.assertEqual(set(lanes), {0, 1, 2, 3}, strategy)
            self.assertEqual(lanes, [lane_for(strategy, rid, 4) for rid in range(4000)])
            counts = [lanes.count(i) for i in range(4)]
            self.assertGreater(min(counts), 600, f"{strategy} is lopsided: {counts}")


if __name__ == "__main__":
    unittest.main()
