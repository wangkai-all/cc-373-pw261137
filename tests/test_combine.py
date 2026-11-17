"""Merge-algebra tests: the four metrics do not share one addition rule."""

import unittest

from skewpipe import combine


def parts_for(metric, chunks):
    """Build per-lane merged states from chunks of source rows."""
    out = []
    for rows in chunks:
        st = combine.new_state(metric)
        for amount, member in rows:
            combine.apply_event(metric, st, {"amount": amount, "member": member})
        out.append(st)
    return out


class MergeAlgebraTests(unittest.TestCase):
    def test_additive_metrics_really_are_additive(self):
        chunks = [[(1, "a"), (2, "b")], [(3, "c")], [(4, "d"), (5, "e")]]
        for metric in ("count", "sum"):
            states = parts_for(metric, chunks)
            merged = combine.new_state(metric)
            for st in states:
                combine.merge_state(metric, merged, st)
            expected = sum(combine.final_value(metric, st) for st in states)
            self.assertEqual(combine.final_value(metric, merged), expected, metric)
            naive = combine.naive_merge(metric, states)
            self.assertTrue(naive["correct"])
            self.assertEqual(naive["value"], expected)

    def test_average_merges_by_fraction_not_mean_of_means(self):
        # lane 1: one row of 100 -> mean 100 · lane 2: three rows of 10 -> mean 10
        states = parts_for("avg", [[(100, "a")], [(10, "b"), (10, "c"), (10, "d")]])
        merged = combine.new_state("avg")
        for st in states:
            combine.merge_state("avg", merged, st)
        self.assertAlmostEqual(combine.final_value("avg", merged), 32.5, places=4)
        naive = combine.naive_merge("avg", states)
        self.assertFalse(naive["correct"])
        self.assertAlmostEqual(naive["value"], 55.0, places=4)  # (100 + 10) / 2

    def test_average_evidence_is_a_correctable_fraction(self):
        st = combine.new_state("avg")
        combine.apply_event("avg", st, {"amount": 8, "member": "x"})
        combine.apply_event("avg", st, {"amount": 12, "member": "y"})
        ev = combine.evidence("avg", st)
        self.assertEqual(ev["kind"], "fraction")
        self.assertEqual((ev["sum"], ev["cnt"]), (20, 2))
        self.assertAlmostEqual(ev["mean"], 10.0)
        # a late row is merged into both numerator and denominator
        late = combine.new_state("avg")
        combine.apply_event("avg", late, {"amount": 40, "member": "z"})
        combine.merge_state("avg", st, late)
        self.assertAlmostEqual(combine.final_value("avg", st), 20.0)

    def test_distinct_merges_by_union_not_sum_of_counts(self):
        # overlapping members across lanes are the whole point
        states = parts_for("distinct", [
            [(1, "a"), (1, "b")],
            [(1, "b"), (1, "c")],
            [(1, "c"), (1, "a")],
        ])
        merged = combine.new_state("distinct")
        for st in states:
            combine.merge_state("distinct", merged, st)
        self.assertEqual(combine.final_value("distinct", merged), 3)
        naive = combine.naive_merge("distinct", states)
        self.assertFalse(naive["correct"])
        self.assertEqual(naive["value"], 6)

    def test_distinct_delta_tracks_only_new_members(self):
        st = combine.new_state("distinct")
        delta = combine.new_delta("distinct")
        for member in ("a", "a", "b"):
            new = combine.apply_event("distinct", st, {"amount": 1, "member": member})
            combine.delta_add("distinct", delta, {"amount": 1, "member": member}, new)
        self.assertEqual(delta["members"], ["a", "b"])
        self.assertEqual(delta["rows"], 3)  # rows still counted even when not new

    def test_delta_rows_are_not_implied_by_distinct_members(self):
        # rows must survive the merge even when the member set adds nothing new
        st = combine.new_state("distinct")
        delta = combine.new_delta("distinct")
        combine.apply_event("distinct", st, {"amount": 1, "member": "a"})
        new = combine.apply_event("distinct", st, {"amount": 1, "member": "a"})
        combine.delta_add("distinct", delta, {"amount": 1, "member": "a"}, new)
        self.assertEqual((delta["members"], delta["rows"]), ([], 1))

    def test_evidence_shapes_are_metric_specific(self):
        by_metric = {}
        for metric in combine.METRIC_ORDER:
            st = combine.new_state(metric)
            combine.apply_event(metric, st, {"amount": 5, "member": "m"})
            by_metric[metric] = combine.evidence(metric, st)
        self.assertEqual(by_metric["count"]["kind"], "counter")
        self.assertEqual(by_metric["sum"]["kind"], "sum")
        self.assertEqual(by_metric["avg"]["kind"], "fraction")
        self.assertEqual(by_metric["distinct"]["kind"], "member-set")
        for ev in by_metric.values():
            self.assertEqual(ev["rows"], 1)

    def test_stable_hash_is_process_independent(self):
        self.assertEqual(combine.stable_hash("HOT#p1"), combine.stable_hash("HOT#p1"))
        self.assertNotEqual(combine.stable_hash("HOT#p1"), combine.stable_hash("HOT#p2"))

    def test_unknown_metric_is_loud(self):
        with self.assertRaises(ValueError):
            combine.new_state("median")


if __name__ == "__main__":
    unittest.main()
