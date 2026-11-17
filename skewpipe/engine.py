"""Deterministic, in-memory streaming pipeline with skew-aware splitting.

Pipeline shape (one ``Run`` advanced by ``Run.step()``)::

    source rows
        │  distributor admits a window, assigns an identity
        ▼
    DistributionService ──► AggregateProcess × N ──► ResultPublisher
        │  naive:    identity = key                  │  merge by metric rule
        │  preroute: identity = key#p(lane)          │  accept only own
        │  split:    identity = key#p(lane)          │  incarnation / live task
        │            + a SplitTask owning the lanes  │
        ▼
    in-flight rows (admitted, not yet processed)

Why the identities matter:

* **naive** sends the whole hot key to one process (``stable_hash(key)``), so
  that process is the bottleneck.
* **preroute** rewrites the key *before* admitting it, so the distributor can
  spread it safely (nothing has been aggregated yet).  It has no way to carry
  evidence that was already aggregated.
* **split** starts while the job is live.  Rows already aggregated under the
  old key must be *carried forward* into the new lanes; rows already admitted
  but not aggregated can be re-keyed in the in-flight window.  The SplitTask is
  the contract that governs both.

Determinism: scheduling uses a virtual clock (one tick = one second); the same
settings always produce the same run, queue sizes and result.
"""

from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Set, Tuple

from . import combine
from .combine import stable_hash
from .events import EventLog, IncarnationRegistry
from .split import SplitTask, SplitTaskRegistry

CODE_VERSION = "skewpipe@1"
TICK_SECONDS = 1.0
AGG_ROWS_PER_TICK = 60.0      # rows one aggregate process can fold per tick
ADMIT_ROWS_PER_TICK = 240     # distributor admission window per tick
PUBLISH_PARTIALS_PER_TICK = 400
INFLIGHT_LATENCY_TICKS = 1     # distributor -> aggregate latency
EMIT_EVERY_TICKS = 3           # partial (evidence) publication cadence
AUTO_SPLIT_TICK = 4            # split mode splits this late, so carry is real
AUTO_SPLIT_SHARE = 0.15        # only keys at least this hot are auto-split

MetricState = Tuple[str, str]  # (metric, identity)


@dataclass
class JobSettings:
    mode: str = "naive"                  # naive | preroute | split
    metric: str = "count"                # count | sum | avg | distinct
    strategy: str = "round_robin"        # round_robin | striped | keyed_lane
    num_aggregators: int = 4
    fanout: int = 4
    num_workers: int = 2
    hot_key: str = "HOT"
    hot_share: float = 0.46
    total_weight: int = 12_000
    seed: int = 7
    threshold: float = 0.02

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> "JobSettings":
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        clean = {k: v for k, v in (data or {}).items() if k in known}
        if clean.get("mode") not in (None, "naive", "preroute", "split"):
            clean["mode"] = "naive"
        if clean.get("metric") not in (None, *combine.METRIC_ORDER):
            clean["metric"] = "count"
        if clean.get("strategy") not in (None, "round_robin", "striped", "keyed_lane"):
            clean["strategy"] = "round_robin"
        obj = cls(**clean)
        obj.num_aggregators = max(1, min(12, int(obj.num_aggregators)))
        obj.fanout = max(1, min(16, int(obj.fanout)))
        obj.num_workers = max(1, min(8, int(obj.num_workers)))
        obj.total_weight = max(800, min(60_000, int(obj.total_weight)))
        obj.hot_share = max(0.0, min(0.95, float(obj.hot_share)))
        obj.threshold = max(0.003, min(0.5, float(obj.threshold)))
        if not str(obj.hot_key).strip():
            obj.hot_key = "HOT"
        return obj

    def signature(self) -> tuple:
        return (self.metric, self.num_aggregators, self.num_workers, self.hot_key,
                round(self.hot_share, 4), self.total_weight, self.seed, self.threshold)


# ---------------------------------------------------------------------------
# source
# ---------------------------------------------------------------------------


class StreamSource:
    """Seeded skewed stream with exact ground truth per key.

    One key takes ``hot_share`` of all rows, four warm keys take 20% together,
    and a long tail fills the rest.  Every key gets a fixed member pool draw;
    members are shared across keys deliberately, so merging distinct counts by
    addition is provably wrong (intersections exist).
    """

    def __init__(self, settings: JobSettings) -> None:
        self.settings = settings
        self.rows = self._build()
        self.expected = self._expected()
        self.row_count = len(self.rows)

    def _build(self) -> List[dict]:
        s = self.settings
        rng = random.Random(s.seed)
        total = s.total_weight
        weights: List[Tuple[str, int]] = []
        if s.hot_share > 0:
            weights.append((s.hot_key, max(1, int(total * s.hot_share))))
        warm_total = int(total * 0.20) if s.hot_share > 0 else int(total * 0.25)
        for key in ("A", "B", "C", "D"):
            weights.append((key, max(1, warm_total // 4)))
        tail_weight = max(1, total - sum(w for _, w in weights))
        n_tail = max(1, min(900, tail_weight // 12))
        zipf = [(i + 1) ** -0.95 for i in range(n_tail)]
        scale = tail_weight / sum(zipf)
        for i in range(n_tail):
            weights.append((f"k{i:04d}", max(1, int(round(zipf[i] * scale)))))

        pool_size = max(16, total // 5)
        members = [f"cust-{i:05d}" for i in range(pool_size)]

        rows: List[dict] = []
        rid = 0
        for key, weight in weights:
            for _ in range(weight):
                rows.append({
                    "rid": rid,
                    "key": key,
                    "amount": rng.randrange(1, 100),
                    "member": members[rng.randrange(pool_size)],
                })
                rid += 1
        rng.shuffle(rows)
        for i, row in enumerate(rows):
            row["rid"] = i
        return rows

    def _expected(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for row in self.rows:
            st = out.setdefault(row["key"], {"count": 0, "sum": 0, "avg": 0.0,
                                             "distinct": 0, "members": set()})
            st["count"] += 1
            st["sum"] += row["amount"]
            st["members"].add(row["member"])
        total_members: Set[str] = set()
        for st in out.values():
            st["avg"] = round(st["sum"] / st["count"], 4) if st["count"] else 0.0
            st["distinct"] = len(st["members"])
            total_members.update(st["members"])
            del st["members"]
        out["__TOTAL__"] = {
            "count": len(self.rows),
            "sum": sum(r["amount"] for r in self.rows),
            "avg": round(sum(r["amount"] for r in self.rows) / max(1, len(self.rows)), 4),
            "distinct": len(total_members),
        }
        return out

    def truth(self, key: str, metric: str):
        st = self.expected.get(key)
        return None if st is None else st.get(metric)

    def row_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in self.rows:
            counts[row["key"]] = counts.get(row["key"], 0) + 1
        return counts

    def skew(self) -> List[dict]:
        total = max(1, self.row_count)
        out = []
        for key, n in sorted(self.row_counts().items(), key=lambda kv: -kv[1]):
            share = n / total
            if share < self.settings.threshold and key != self.settings.hot_key:
                continue
            out.append({
                "key": key,
                "rows": n,
                "share": round(share, 4),
                "single_process_ticks": round(n / AGG_ROWS_PER_TICK, 1),
                "suggested_fanout": max(2, min(16, int(n / (AGG_ROWS_PER_TICK * 2)))),
            })
        return out


def lane_for(strategy: str, rid: int, fanout: int) -> int:
    if strategy == "round_robin":
        return rid % fanout
    if strategy == "striped":
        return stable_hash(f"stripe:{rid}") % fanout
    return stable_hash(f"lane:{rid // 7}") % fanout  # keyed-ish salt


# ---------------------------------------------------------------------------
# processes
# ---------------------------------------------------------------------------


class Process:
    kind = "process"

    def __init__(self, name: str, inc: int = 0) -> None:
        self.name = name
        self.inc = inc
        self.busy = 0.0
        self.work_total = 0.0
        self.restarts = 0

    def state_entries(self) -> int:
        return 0

    def state_bytes(self) -> int:
        return 0


class AggregateProcess(Process):
    """Owns a partition of identities; retains local evidence per identity."""

    kind = "aggregate"

    def __init__(self, idx: int, inc: int) -> None:
        super().__init__(f"agg-{idx}", inc)
        self.idx = idx
        self.queue: Deque[dict] = deque()
        self.state: Dict[MetricState, dict] = {}
        self.delta: Dict[MetricState, dict] = {}
        self.unemitted: List[dict] = []  # raw rows folded since last emit
        self.touched: Set[MetricState] = set()
        self.receipts: List[dict] = []
        self.rows_in = 0
        self.rows_processed = 0
        self.traffic_in = 0
        self.emits = 0
        self.suspended = False

    def enqueue(self, event: dict) -> None:
        self.offer(event)

    def offer(self, event: dict) -> None:
        """Admit a row into this process (counts as traffic in)."""

        self.queue.append(event)
        self.rows_in += 1
        self.traffic_in += 1

    def state_entries(self) -> int:
        return len(self.state)

    def state_bytes(self) -> int:
        return sum(combine.state_bytes(m, st) for (m, _), st in self.state.items())

    def dispose_identity(self, metric: str, identity: str) -> Optional[dict]:
        rk = (metric, identity)
        st = self.state.pop(rk, None)
        self.delta.pop(rk, None)
        self.touched.discard(rk)
        return st

    def top_identities(self, metric: str, n: int = 3) -> List[dict]:
        rows = [(combine.final_value(metric, st), ident) for (m, ident), st in self.state.items()
                if m == metric and st["rows"]]
        rows.sort(reverse=True)
        return [{"identity": ident, "value": value} for value, ident in rows[:n]]


class ResultPublisher(Process):
    """Stateless merge stage; rejects partials outside its own contract."""

    kind = "result"

    def __init__(self, inc: int) -> None:
        super().__init__("result", inc)
        self.queue: Deque[dict] = deque()
        self.state: Dict[MetricState, dict] = {}
        self.seen_events: Set[str] = set()
        self.accepted = 0
        self.rejected: List[dict] = []
        self.receipts: List[dict] = []

    def state_entries(self) -> int:
        return len(self.state)

    def state_bytes(self) -> int:
        return sum(combine.state_bytes(m, st) for (m, _), st in self.state.items())

    def dispose_identity(self, metric: str, identity: str) -> Optional[dict]:
        return self.state.pop((metric, identity), None)


class DistributionService(Process):
    """Source reader + router.  Owns the in-flight (admitted, not aggregated) rows."""

    kind = "distributor"

    def __init__(self, num_aggregators: int) -> None:
        super().__init__("distributor", 0)
        self.num_aggregators = num_aggregators
        self.inflight: Dict[str, Deque[dict]] = {}
        self.admitted = 0
        self.traffic = 0

    def target_for(self, identity: str, explicit: Optional[int] = None) -> int:
        if explicit is not None:
            return explicit % max(1, self.num_aggregators)
        return stable_hash(identity) % max(1, self.num_aggregators)

    def pending(self) -> int:
        return sum(len(q) for q in self.inflight.values())


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


class Run:
    def __init__(self, settings: JobSettings | dict) -> None:
        self.settings = settings if isinstance(settings, JobSettings) else JobSettings.from_dict(settings)
        self.source = StreamSource(self.settings)
        self.inc = IncarnationRegistry.next()
        self.code = CODE_VERSION
        self.events = EventLog()
        self.distributor = DistributionService(self.settings.num_aggregators)
        self.aggs = [AggregateProcess(i, self.inc) for i in range(self.settings.num_aggregators)]
        self.publisher = ResultPublisher(self.inc)
        self.tasks = SplitTaskRegistry(self.inc)
        self.split_info: Dict[str, dict] = {}
        self.split_status: Dict[str, str] = {}
        self.lane_index: Dict[str, str] = {}  # lane identity -> split_id that owns it
        self.seen_counts: Dict[str, int] = {}  # observed base-key counts (skew detection)
        self.cursor = 0
        self.tick = 0
        self.now = 0.0
        self.finished = False
        self.final_tick: Optional[int] = None
        self.history: List[dict] = []
        self._baseline_cache: Optional[dict] = None
        self._emit_seq = 0
        self._suspended = False
        self.orphaned: List[dict] = []       # rows lost to a process exit, replayable
        self.orphaned_replayed = 0
        self.needs_rebuild = False
        self.note = "run created"
        self.events.add(
            "system", "run",
            f"inc {self.inc} · mode={self.settings.mode} · metric={self.settings.metric} · "
            f"aggs={self.settings.num_aggregators} · fanout={self.settings.fanout}",
            t=0.0,
        )

    # -- ingestion ---------------------------------------------------------
    def _identity_for(self, row: dict) -> Tuple[str, Optional[SplitTask], Optional[int]]:
        base = row["key"]
        s = self.settings
        self.seen_counts[base] = self.seen_counts.get(base, 0) + 1
        if s.mode == "naive":
            return base, None, None
        if s.mode == "preroute":
            if base != s.hot_key or s.hot_share <= 0:
                return base, None, None
            lane = lane_for(s.strategy, row["rid"], s.fanout)
            ident = f"{base}#p{lane}"
            # preroute can place the lane explicitly: the distributor decided
            # before the row was ever admitted, so there is no contract to keep.
            return ident, None, lane
        task = self.tasks.active_for(base)
        if task is None:
            return base, None, None
        lane = lane_for(task.strategy, row["rid"], task.fanout)
        return f"{base}#p{lane}", task, None

    def admit(self) -> None:
        budget = ADMIT_ROWS_PER_TICK
        while budget > 0 and self.cursor < self.source.row_count:
            row = self.source.rows[self.cursor]
            identity, task, explicit = self._identity_for(row)
            event = {
                "rid": row["rid"],
                "base": row["key"],
                "identity": identity,
                "amount": row["amount"],
                "member": row["member"],
                "split_id": task.split_id if task else None,
                "admitted_tick": self.tick,
            }
            self.distributor.inflight.setdefault(identity, deque()).append(event)
            self.distributor.admitted += 1
            self.distributor.traffic += 1
            self.cursor += 1
            budget -= 1

    def dispatch(self) -> None:
        s = self.settings
        for identity, queue in list(self.distributor.inflight.items()):
            while queue and self.tick - queue[0]["admitted_tick"] >= INFLIGHT_LATENCY_TICKS:
                event = queue.popleft()
                explicit = None
                info = self.split_info.get(event["base"])
                if "#p" in identity and info and info["split_id"] == event.get("split_id"):
                    # a lane is *placed*: lane i lives on aggregate (i % N), so a
                    # split actually spreads work instead of re-hashing the lane
                    # name into another hot partition.
                    explicit = int(identity.rsplit("#p", 1)[1])
                elif s.mode == "preroute" and "#p" in identity and event["base"] == s.hot_key:
                    explicit = int(identity.rsplit("#p", 1)[1])
                idx = self.distributor.target_for(identity, explicit)
                self.aggs[idx].enqueue(event)
            if not queue:
                del self.distributor.inflight[identity]

    # -- aggregation -------------------------------------------------------
    def process(self) -> None:
        metric = self.settings.metric
        for agg in self.aggs:
            agg.busy = 0.0
            if agg.suspended:
                continue
            capacity = AGG_ROWS_PER_TICK + max(0, self.settings.num_workers - 1) * 15.0
            processed = 0
            while agg.queue and processed < capacity:
                event = agg.queue.popleft()
                self._aggregate_event(agg, metric, event)
                processed += 1
            agg.busy = round(processed / AGG_ROWS_PER_TICK, 2)
            agg.work_total += processed
            if processed:
                agg.rows_processed += processed
            if self.tick % EMIT_EVERY_TICKS == 0:
                self._emit_deltas(agg)

    def _aggregate_event(self, agg: AggregateProcess, metric: str, event: dict) -> None:
        rk = (metric, event["identity"])
        state = agg.state.get(rk)
        if state is None:
            state = combine.new_state(metric)
            agg.state[rk] = state
        delta = agg.delta.get(rk)
        if delta is None:
            delta = combine.new_delta(metric)
            agg.delta[rk] = delta
        new_members = combine.apply_event(metric, state, event)
        combine.delta_add(metric, delta, event, new_members)
        agg.touched.add(rk)
        agg.unemitted.append(event)
        if len(agg.unemitted) > 20_000:
            del agg.unemitted[: len(agg.unemitted) - 20_000]

    def _emit_deltas(self, agg: AggregateProcess) -> None:
        for rk, delta in list(agg.delta.items()):
            if delta["rows"] == 0:
                continue
            metric, identity = rk
            self._emit_seq += 1
            partial = {
                "event_id": f"{agg.name}@{agg.inc}:{self._emit_seq}",
                "metric": metric,
                "identity": identity,
                "base": identity.split("#p")[0],
                "inc": agg.inc,
                "code": self.code,
                "split_id": self._split_id_for(identity),
                "lane": self._lane_of(identity),
                "rows": delta["rows"],
                "evidence": combine.evidence(metric, delta),
                "delta": delta,  # in-memory payload, never serialized directly
            }
            agg.receipts.append(partial)
            if len(agg.receipts) > 24:
                del agg.receipts[: len(agg.receipts) - 24]
            self.publisher.queue.append(partial)
            agg.emits += 1
            agg.delta[rk] = combine.new_delta(metric)
        agg.touched.clear()
        agg.unemitted.clear()

    @staticmethod
    def _lane_of(identity: str) -> Optional[int]:
        if "#p" not in identity:
            return None
        try:
            return int(identity.rsplit("#p", 1)[1])
        except ValueError:
            return None

    def _split_id_for(self, identity: str) -> Optional[str]:
        """The split task whose lane contract an identity belongs to, if any."""

        if "#p" not in identity:
            return None
        base = identity.split("#p")[0]
        info = self.split_info.get(base)
        if not info:
            return None
        return None if info["retired"] else info["split_id"]

    # -- publishing --------------------------------------------------------
    def publish(self) -> None:
        budget = PUBLISH_PARTIALS_PER_TICK
        applied = 0
        while self.publisher.queue and budget > 0:
            partial = self.publisher.queue.popleft()
            self._publish_one(partial)
            budget -= 1
            applied += 1
        self.publisher.busy = round(applied / PUBLISH_PARTIALS_PER_TICK, 2)
        self.publisher.work_total += applied

    def _publish_one(self, partial: dict) -> None:
        reason = self._reject_reason(partial)
        if reason:
            self.publisher.rejected.append({
                "reason": reason,
                "identity": partial.get("identity"),
                "event_id": partial.get("event_id"),
                "inc": partial.get("inc"),
                "split_id": partial.get("split_id"),
            })
            if len(self.publisher.rejected) > 200:
                del self.publisher.rejected[: len(self.publisher.rejected) - 200]
            return
        metric = partial["metric"]
        identity = partial["identity"]
        rk = (metric, identity)
        state = self.publisher.state.get(rk)
        if state is None:
            state = combine.new_state(metric)
            self.publisher.state[rk] = state
        combine.merge_delta(metric, state, partial["delta"])
        self.publisher.seen_events.add(partial["event_id"])
        self.publisher.accepted += 1
        self.publisher.receipts.append(partial)
        if len(self.publisher.receipts) > 24:
            del self.publisher.receipts[: len(self.publisher.receipts) - 24]

    def _reject_reason(self, partial: dict) -> Optional[str]:
        if partial.get("inc") != self.publisher.inc:
            return "foreign-incarnation"
        if partial.get("code") != self.code:
            return "stale-code"
        split_id = partial.get("split_id")
        if split_id:
            task = self.tasks.get(split_id)
            if task is None:
                return "unknown-split"
            if task.retired:
                return "retired-split"
            if not task.expected_identity(partial["identity"]):
                return "lane-mismatch"
        if partial["event_id"] in self.publisher.seen_events:
            return "duplicate-publish"
        return None

    # -- split control -----------------------------------------------------
    def start_split(self, base_key: str, strategy: Optional[str] = None,
                    fanout: Optional[int] = None, reason: str = "manual") -> dict:
        """Split a live key into lanes without losing or double-counting evidence.

        Order is the guarantee:

        1. flush every aggregate delta and drain the publisher queue, so all
           evidence for *processed* rows lives in exactly one place (publisher);
        2. merge that evidence under the metric rule into ``carry``;
        3. retire the old task (its lane names are now closed);
        4. purge the old identities from publisher + aggregates;
        5. re-key rows that were admitted but **not** aggregated (queues and
           in-flight window) into the new lanes -- these carry no evidence yet;
        6. publish ``carry`` as lane partials under the new task.
        """

        s = self.settings
        strategy = strategy or s.strategy
        fanout = max(1, int(fanout or s.fanout))
        existing = self.tasks.active_for(base_key)

        self._flush_all()
        carry = self._collect_carry(base_key, existing)

        plan = {str(i): {"rows": 0} for i in range(fanout)}
        for row in self.source.rows:
            if row["key"] != base_key:
                continue
            plan[str(lane_for(strategy, row["rid"], fanout))]["rows"] += 1
        task = SplitTask(
            split_id=f"split-{len(self.tasks.tasks) + 1:02d}",
            base_key=base_key, inc=self.inc, strategy=strategy, fanout=fanout,
            partitions=plan, reason=reason,
        )
        self.tasks.add(task)
        if existing is not None:
            self._retire(existing)

        old_lanes = existing.lanes() if existing is not None else []
        self._purge_identities([base_key, *old_lanes], old_lanes)
        requeued = self._requeue_pending(base_key, old_lanes, task)
        carry_rows = self._emit_carry(task, carry)

        self.split_info[base_key] = {
            "split_id": task.split_id,
            "base_key": base_key,
            "strategy": strategy,
            "fanout": fanout,
            "lanes": task.lanes(),
            "epoch": task.epoch,
            "reason": reason,
            "retired": False,
            "carried_rows": carry_rows,
            "requeued_rows": requeued,
        }
        self.split_status[base_key] = "active"
        task.merged_rows = carry_rows
        self.events.add(
            "split", "split-task",
            f"{task.split_id} epoch {task.epoch} · {base_key} → {fanout} lanes · "
            f"{strategy} · carried {carry_rows} processed rows · re-keyed {requeued} "
            f"unprocessed rows · reason={reason}",
            t=self.now,
        )
        return task.snapshot()

    def _flush_all(self) -> None:
        """Force all buffered evidence out to the publisher, then drain it."""

        for agg in self.aggs:
            self._emit_deltas(agg)
        self.publisher.busy = 0.0
        guard = 0
        while self.publisher.queue and guard < 10_000:
            self._publish_one(self.publisher.queue.popleft())
            guard += 1

    def _retire(self, task: SplitTask) -> None:
        task.retired = True
        info = self.split_info.get(task.base_key)
        if info and info["split_id"] == task.split_id:
            info["retired"] = True
            self.split_status[task.base_key] = "superseded"

    def _collect_carry(self, base_key: str, old: Optional[SplitTask]) -> dict:
        """Merge all published evidence for the key (and its old lanes)."""

        metric = self.settings.metric
        carry = combine.new_state(metric)
        ids = [base_key]
        if old is not None:
            ids.extend(old.lanes())
        for identity in ids:
            st = self.publisher.state.get((metric, identity))
            if st is not None and st["rows"]:
                combine.merge_state(metric, carry, st)
        return carry

    def _purge_identities(self, identities: List[str], old_lanes: List[str]) -> None:
        metric = self.settings.metric
        for agg in self.aggs:
            for identity in identities:
                agg.dispose_identity(metric, identity)
        for identity in identities:
            self.publisher.dispose_identity(metric, identity)

    def _requeue_pending(self, base_key: str, old_lanes: List[str],
                         task: SplitTask) -> int:
        """Re-key admitted-but-unaggregated rows into the new lanes.

        No evidence exists for these rows, so re-keying them cannot duplicate
        anything.  Rows are moved, never copied: each aggregate's queue is
        rebuilt, and queued-for-another-key rows are left untouched.
        """

        old_ids = {base_key, *old_lanes}
        moved: List[dict] = []
        for agg in self.aggs:
            keep: Deque[dict] = deque()
            for ev in agg.queue:
                if ev["identity"] in old_ids:
                    lane = lane_for(task.strategy, ev["rid"], task.fanout)
                    ev["identity"] = f"{base_key}#p{lane}"
                    ev["split_id"] = task.split_id
                    moved.append(ev)
                else:
                    keep.append(ev)
            agg.queue = keep
        for identity in list(self.distributor.inflight.keys()):
            if identity not in old_ids:
                continue
            q = self.distributor.inflight.pop(identity)
            while q:
                ev = q.popleft()
                lane = lane_for(task.strategy, ev["rid"], task.fanout)
                ev["identity"] = f"{base_key}#p{lane}"
                ev["split_id"] = task.split_id
                moved.append(ev)
        for ev in moved:
            idx = self.distributor.target_for(ev["identity"], self._lane_of(ev["identity"]))
            self.aggs[idx].offer(ev)
        if moved:
            self.events.add("split", "remap-inflight",
                            f"{task.split_id} re-keyed {len(moved)} unaggregated rows "
                            f"(no evidence exists for them yet)", t=self.now)
        return len(moved)

    def _emit_carry(self, task: SplitTask, carry: dict) -> int:
        """Redistribute carried evidence across the new lanes and publish it.

        Counts and sums split exactly.  Means split numerator with denominator
        so ``Σsum / Σcnt`` is preserved.  Distinct sets partition members, so
        the union (and therefore the final count) is preserved.
        """

        metric = self.settings.metric
        rows = carry["rows"]
        if rows <= 0:
            return 0
        lanes = [combine.new_state(metric) for _ in range(task.fanout)]
        if metric == "distinct":
            members = sorted(carry["members"])
            for i, member in enumerate(members):
                lanes[i % task.fanout]["members"].add(member)
            base, extra = divmod(rows, task.fanout)
            for i, st in enumerate(lanes):
                st["rows"] = base + (1 if i < extra else 0)
            reported = rows
        elif metric == "avg":
            total_sum = carry["sum"]
            base, extra = divmod(rows, task.fanout)
            acc = 0
            for i, st in enumerate(lanes):
                cnt = base + (1 if i < extra else 0)
                st["cnt"] = cnt
                st["rows"] = cnt
                if i == task.fanout - 1:
                    st["sum"] = total_sum - acc
                else:
                    st["sum"] = int(round(total_sum * cnt / rows))
                    acc += st["sum"]
            reported = rows
        else:
            base, extra = divmod(rows, task.fanout)
            total_sum = carry.get("sum", 0)
            sbase, sextra = divmod(total_sum, task.fanout)
            acc = 0
            for i, st in enumerate(lanes):
                n = base + (1 if i < extra else 0)
                st["rows"] = n
                st["n"] = n
                if metric == "sum":
                    s = sbase + (1 if i < sextra else 0)
                    st["sum"] = s
                    acc += s
            if metric == "sum" and lanes:
                lanes[-1]["sum"] += total_sum - acc
            reported = rows

        for i, st in enumerate(lanes):
            if st["rows"] == 0 and not (metric == "distinct" and st["members"]):
                continue
            identity = task.lane_identity(str(i))
            self.lane_index[identity] = task.split_id
            # the carry is adopted into the lane's local state *and* published,
            # so the aggregate's retained state stays a complete description of
            # the lane (a publisher rebuild can be reconstructed from it).
            idx = self.distributor.target_for(identity, i)
            agg = self.aggs[idx]
            rk = (metric, identity)
            local = agg.state.get(rk)
            if local is None:
                local = combine.new_state(metric)
                agg.state[rk] = local
            combine.merge_delta(metric, local, st)
            agg.delta[rk] = combine.new_delta(metric)
            self.publisher.queue.appendleft({
                "event_id": f"carry:{task.split_id}:{i}",
                "metric": metric,
                "identity": identity,
                "base": task.base_key,
                "inc": self.inc,
                "code": self.code,
                "split_id": task.split_id,
                "lane": i,
                "rows": st["rows"],
                "evidence": combine.evidence(metric, st),
                "delta": st,
            })
        self.events.add("split", "carry",
                        f"{task.split_id} carried {reported} processed rows of "
                        f"{task.base_key} into {task.fanout} lane partials", t=self.now)
        return reported

    def retrigger(self, base_key: str, strategy: Optional[str] = None,
                  fanout: Optional[int] = None) -> dict:
        snapshot = self.start_split(base_key, strategy, fanout, reason="retrigger")
        return {"ok": True, "split": snapshot}

    # -- failure / duplication controls -----------------------------------
    def kill(self, name: str) -> dict:
        """Exit a process: its un-emitted evidence is lost, queued rows are too.

        Evidence already published survives (the publisher has it).  The lost
        rows are kept in ``orphaned`` so the run can be replayed from them --
        which is the recovery story a real pipeline needs (checkpointed source /
        durable upstream), not a claim that failure is free.
        """

        for i, agg in enumerate(self.aggs):
            if agg.name != name:
                continue
            lost_rows = len(agg.unemitted)
            lost_entries = agg.state_entries()
            queued = len(agg.queue)
            self.orphaned.extend({"identity": ev["identity"], "base": ev["base"],
                                  "rid": ev["rid"], "amount": ev["amount"],
                                  "member": ev["member"], "split_id": ev.get("split_id")}
                                 for ev in agg.unemitted)
            self.orphaned.extend({"identity": ev["identity"], "base": ev["base"],
                                  "rid": ev["rid"], "amount": ev["amount"],
                                  "member": ev["member"], "split_id": ev.get("split_id")}
                                 for ev in agg.queue)
            new = AggregateProcess(i, self.inc)
            new.restarts = agg.restarts + 1
            self.aggs[i] = new
            self.events.add(
                "fail", "kill",
                f"{name} exited · {lost_entries} state entries lost · {lost_rows + queued} rows "
                f"not yet published ({lost_rows} folded-but-unemitted + {queued} queued) · "
                f"restarted empty, same incarnation {self.inc}",
                t=self.now,
            )
            self.note = f"{name} exited: {lost_rows + queued} rows lost before publishing"
            return {"ok": True, "killed": name, "lost_state_entries": lost_entries,
                    "lost_rows": lost_rows + queued, "dropped_queued": queued,
                    "recoverable": True}
        return {"ok": False, "error": f"no process named {name}"}

    def replay_orphaned(self) -> dict:
        """Re-route the rows a crashed process never published (recovery)."""

        if not self.orphaned:
            return {"ok": True, "queued": 0, "note": "nothing to replay"}
        rows = list(self.orphaned)
        self.orphaned.clear()
        self.finished = False
        self.final_tick = None
        self._baseline_cache = None
        self._emit_seq += 1
        self.orphaned_replayed += len(rows)
        for ev in rows:
            idx = self.distributor.target_for(ev["identity"], self._lane_of(ev["identity"]))
            self.aggs[idx].offer({
                "rid": ev["rid"], "base": ev["base"], "identity": ev["identity"],
                "amount": ev["amount"], "member": ev["member"],
                "split_id": ev.get("split_id"), "admitted_tick": self.tick,
            })
        self.events.add("recover", "replay-orphaned",
                        f"replayed {len(rows)} lost rows from a durable source window", t=self.now)
        return {"ok": True, "queued": len(rows)}

    def kill_all(self) -> dict:
        total_entries = sum(a.state_entries() for a in self.aggs)
        total_rows = sum(sum(st["rows"] for st in a.state.values()) for a in self.aggs)
        self.aggs = [AggregateProcess(i, self.inc) for i in range(self.settings.num_aggregators)]
        self.events.add("fail", "kill-all",
                        f"all aggregates restarted · {total_entries} state entries / "
                        f"{total_rows} rows lost", t=self.now)
        return {"ok": True, "restarted": len(self.aggs), "lost_state_entries": total_entries,
                "lost_rows": total_rows}

    def kill_publisher(self) -> dict:
        self.publisher = ResultPublisher(self.inc)
        self.needs_rebuild = True
        self.events.add("fail", "kill-publisher",
                        "result publisher restarted · merged state discarded · "
                        "aggregates will re-emit their retained state to rebuild it", t=self.now)
        return {"ok": True, "killed": "result"}

    def _rebuild_publisher(self) -> int:
        """Re-emit each aggregate's retained local state as one full partial.

        The rebuilt publisher must equal the union of aggregate state exactly.
        Because that union includes rows still sitting in an aggregate's
        un-emitted delta, those deltas are consumed by the rebuild -- otherwise
        the next normal emit would add the same rows a second time.
        """

        self.needs_rebuild = False
        self._emit_seq += 1
        fresh = ResultPublisher(self.inc)
        emitted = 0
        for agg in self.aggs:
            for (metric, identity), st in agg.state.items():
                if not st["rows"]:
                    continue
                self._emit_seq += 1
                fresh.queue.append({
                    "event_id": f"rebuild:{agg.name}@{agg.inc}:{self._emit_seq}",
                    "metric": metric, "identity": identity,
                    "base": identity.split("#p")[0], "inc": agg.inc, "code": self.code,
                    "split_id": self._split_id_for(identity),
                    "lane": self._lane_of(identity),
                    "rows": st["rows"], "evidence": combine.evidence(metric, st),
                    "delta": combine.copy_state(metric, st),
                })
                emitted += 1
            # the rebuild just published everything this aggregate had folded
            agg.delta = {rk: combine.new_delta(rk[0]) for rk in agg.state}
            agg.unemitted.clear()
            agg.touched.clear()
        merged: Dict[MetricState, dict] = {}
        while fresh.queue:
            partial = fresh.queue.popleft()
            if self._reject_reason(partial):
                continue
            metric = partial["metric"]
            identity = partial["identity"]
            rk = (metric, identity)
            target = merged.get(rk)
            if target is None:
                target = combine.new_state(metric)
                merged[rk] = target
            combine.merge_delta(metric, target, partial["delta"])
            fresh.seen_events.add(partial["event_id"])
        self.publisher.state = merged
        self.publisher.seen_events |= fresh.seen_events
        self.events.add("recover", "rebuild-publisher",
                        f"rebuilt publisher from {emitted} aggregate states", t=self.now)
        return emitted

    def suspend(self, name: str, value: bool = True) -> dict:
        for agg in self.aggs:
            if agg.name == name:
                agg.suspended = value
                self.events.add("fail", "suspend" if value else "resume",
                                f"{name} {'suspended' if value else 'resumed'}", t=self.now)
                return {"ok": True, "process": name, "suspended": value}
        return {"ok": False, "error": f"no process named {name}"}

    def duplicate_publish(self, name: str = "") -> dict:
        """Replay a live aggregate's last partials verbatim (retry / at-least-once)."""

        chosen = [a for a in self.aggs if not name or a.name == name]
        parts = [dict(p) for a in chosen for p in a.receipts[-4:]]
        for p in parts:
            self.publisher.queue.append(p)
        self.events.add("publisher", "duplicate",
                        f"replayed {len(parts)} live partials verbatim · same event_id → "
                        f"the publisher must dedupe", t=self.now)
        return {"ok": True, "queued": len(parts)}

    def replay_old_incarnation(self) -> dict:
        """Replay partials from a previous incarnation (e.g. after a restart)."""

        parts = []
        for a in self.aggs:
            for p in a.receipts[-4:]:
                old = dict(p)
                old["inc"] = self.inc - 1
                old["code"] = "skewpipe@0"
                old["event_id"] = f"old-inc{self.inc - 1}:" + p["event_id"]
                parts.append(old)
        if not parts:
            # fabricate a plausible pre-restart partial so the check is testable
            parts = [self._fabricated_old_partial()]
        for p in parts:
            self.publisher.queue.append(p)
        self.events.add("publisher", "replay-old",
                        f"queued {len(parts)} partials from incarnation {self.inc - 1}", t=self.now)
        return {"ok": True, "queued": len(parts)}

    def _fabricated_old_partial(self) -> dict:
        metric = self.settings.metric
        st = combine.new_state(metric)
        if metric in ("count", "sum"):
            st["rows"] = 7
            st["n"] = 7
            if metric == "sum":
                st["sum"] = 777
        elif metric == "avg":
            st["rows"] = st["cnt"] = 7
            st["sum"] = 777
        else:
            st["members"] = {f"ghost-{i}" for i in range(7)}
            st["rows"] = 7
        return {
            "event_id": f"old-inc{self.inc - 1}:fabricated", "metric": metric,
            "identity": self.settings.hot_key, "base": self.settings.hot_key,
            "inc": self.inc - 1, "code": "skewpipe@0", "split_id": None,
            "rows": 7, "evidence": combine.evidence(metric, st), "delta": st,
        }

    def replay_stale_split(self) -> dict:
        """Replay partials tagged with a split task that has since been retired."""

        retired = {t.split_id for t in self.tasks.tasks if t.retired}
        parts = []
        for a in self.aggs:
            for p in a.receipts:
                if p.get("split_id") in retired:
                    parts.append(dict(p))
        if not parts:
            # synthetic stale lane partial from the retired task, if any
            old = next((t for t in self.tasks.tasks if t.retired), None)
            if old is not None:
                st = combine.new_state(self.settings.metric)
                metric = self.settings.metric
                st["rows"] = 5
                st["n"] = 5
                if metric == "sum":
                    st["sum"] = 555
                elif metric == "avg":
                    st["cnt"] = 5
                    st["sum"] = 555
                elif metric == "distinct":
                    st["members"] = {f"stale-{i}" for i in range(5)}
                parts.append({
                    "event_id": f"stale:{old.split_id}", "metric": metric,
                    "identity": old.lanes()[0], "base": old.base_key,
                    "inc": old.inc, "code": self.code, "split_id": old.split_id,
                    "rows": 5, "evidence": combine.evidence(metric, st), "delta": st,
                })
        for p in parts:
            self.publisher.queue.append(p)
        self.events.add("publisher", "replay-stale-split",
                        f"queued {len(parts)} partials from retired split task(s)", t=self.now)
        return {"ok": True, "queued": len(parts)}

    # -- run loop ----------------------------------------------------------
    def step(self) -> dict:
        if self.finished:
            return self.snapshot()
        self.tick += 1
        self.now = round(self.tick * TICK_SECONDS, 3)
        if getattr(self, "needs_rebuild", False):
            self._rebuild_publisher()
        self._maybe_auto_split()
        self.admit()
        self.dispatch()
        self.process()
        self.publish()
        self._record_history()
        self._check_finished()
        return self.snapshot()

    def _maybe_auto_split(self) -> None:
        """Split-mode behaviour: detect skew from observed rows, then re-key.

        Detection is from the stream itself (rows seen so far), not from the
        source's ground truth; the split then happens live, with real carry.
        """

        if self.settings.mode != "split":
            return
        if self.tick < AUTO_SPLIT_TICK:
            return
        observed = max(1, self.distributor.admitted)
        candidates = [
            (key, n) for key, n in self.seen_counts.items()
            if n / observed >= AUTO_SPLIT_SHARE and self.tasks.active_for(key) is None
        ]
        if not candidates:
            return
        key, n = max(candidates, key=lambda kv: kv[1])
        share = n / observed
        self.events.add("split", "skew-detected",
                        f"hot key {key}: {n}/{observed} observed rows ({share:.0%}) "
                        f"≥ threshold {AUTO_SPLIT_SHARE:.0%}", t=self.now)
        self.start_split(key, reason="auto-detect")

    def run_to_end(self, max_ticks: int = 6000) -> None:
        while not self.finished and self.tick < max_ticks:
            self.step()
        if not self.finished:
            self.finished = True
            self.events.add("system", "timeout", f"stopped at {self.tick} ticks", t=self.now)

    def _check_finished(self) -> None:
        if self.cursor < self.source.row_count:
            return
        if self.distributor.pending():
            return
        if any(a.queue for a in self.aggs):
            return
        if any(any(st["rows"] for st in a.delta.values()) for a in self.aggs):
            return
        if self.publisher.queue:
            return
        self.finished = True
        self.final_tick = self.tick

    def _processed_rows(self) -> int:
        """Rows whose *evidence* exists somewhere durable, not lifetime counters.

        A process exit destroys its counters, but the rows it folded are either
        already merged in the publisher, queued as a partial, or still sitting
        in an aggregate delta.  Counting evidence is what makes progress recover
        after a crash and lets it reach exactly ``row_count`` at the end.
        """

        merged = sum(st["rows"] for st in self.publisher.state.values())
        # a duplicate or foreign partial waiting in the queue is not new work:
        # it will be rejected (or was already merged), so it must not inflate
        # progress.  Only count queued evidence the publisher will accept.
        queued = sum(p["rows"] for p in self.publisher.queue
                     if p.get("inc") == self.publisher.inc
                     and p.get("code") == self.code
                     and p["event_id"] not in self.publisher.seen_events)
        folded = sum(d["rows"] for a in self.aggs for d in a.delta.values())
        return merged + queued + folded

    def _record_history(self) -> None:
        total_processed = self._processed_rows()
        total_bytes = 0
        max_queue = 0
        for a in self.aggs:
            total_bytes += a.state_bytes()
            max_queue = max(max_queue, len(a.queue))
        self.history.append({
            "tick": self.tick,
            "processed": total_processed,
            "admitted": self.distributor.admitted,
            "accepted": self.publisher.accepted,
            "rejected": len(self.publisher.rejected),
            "state_kb": round(total_bytes / 1024, 1),
            "max_queue": max_queue,
            "inflight": self.distributor.pending(),
            "queues": [len(a.queue) for a in self.aggs],
            "traffic": [a.traffic_in for a in self.aggs],
            "states": [a.state_entries() for a in self.aggs],
            "state_bytes": [a.state_bytes() for a in self.aggs],
        })
        if len(self.history) > 4000:
            del self.history[: len(self.history) - 4000]

    # -- views -------------------------------------------------------------
    def snapshot(self) -> dict:
        progress = {
            "total": self.source.row_count,
            "admitted": self.distributor.admitted,
            "inflight": self.distributor.pending(),
            "queued": sum(len(a.queue) for a in self.aggs),
            "processed": self._processed_rows(),
            "published_partials": self.publisher.accepted,
            "rejected_partials": len(self.publisher.rejected),
            "undrained": self.source.row_count - self.cursor,
        }
        procs = []
        for a in self.aggs:
            procs.append({
                "name": a.name,
                "kind": a.kind,
                "inc": a.inc,
                "queue": len(a.queue),
                "rows_in": a.rows_in,
                "rows_processed": a.rows_processed,
                "traffic": a.traffic_in,
                "emitted": a.emits,
                "state_entries": a.state_entries(),
                "state_bytes": a.state_bytes(),
                "busy": a.busy,
                "restarts": a.restarts,
                "suspended": a.suspended,
                "stalled": a.suspended and bool(a.queue),
                "top": a.top_identities(self.settings.metric),
            })
        return {
            "inc": self.inc,
            "code": self.code,
            "tick": self.tick,
            "now": self.now,
            "finished": self.finished,
            "final_tick": self.final_tick,
            "note": self.note,
            "settings": vars(self.settings),
            "progress": progress,
            "skew": self.source.skew(),
            "processes": procs,
            "publisher": {
                "name": "result",
                "inc": self.publisher.inc,
                "queue": len(self.publisher.queue),
                "accepted": self.publisher.accepted,
                "rejected": len(self.publisher.rejected),
                "reject_reasons": self._reject_histogram(),
                "state_entries": self.publisher.state_entries(),
                "state_bytes": self.publisher.state_bytes(),
                "busy": self.publisher.busy,
            },
            "splits": {
                "status": self.split_status,
                "info": self.split_info,
                "tasks": self.tasks.all_snapshot(),
            },
            "events": self.events.tail(70),
            "history": self.history[-120:],
            "baseline": self._baseline_summary(),
        }

    def _reject_histogram(self) -> Dict[str, int]:
        hist: Dict[str, int] = {}
        for r in self.publisher.rejected:
            hist[r["reason"]] = hist.get(r["reason"], 0) + 1
        return hist

    def _baseline_summary(self) -> Optional[dict]:
        """Naive no-split run on the same source, compared head-to-head.

        Computed on demand (a second in-memory run) and cached.  It is the
        counterfactual the dashboard shows next to the split run.
        """

        cache = getattr(self, "_baseline_cache", None)
        if cache is not None:
            return cache
        if self.settings.mode == "naive":
            # the current run *is* the baseline: recompute each call, never cache
            return {
                "mode": "naive",
                "same_as_current": True,
                "ticks": self.final_tick or self.tick,
                "max_state_bytes": max((a.state_bytes() for a in self.aggs), default=0),
                "max_traffic": max((a.traffic_in for a in self.aggs), default=0),
                "peak_queue": max((h["max_queue"] for h in self.history), default=0),
                "history": self.history[-400:],
            }
        else:
            base_settings = JobSettings.from_dict({
                **vars(self.settings), "mode": "naive",
            })
            shadow = Run(base_settings)
            shadow.run_to_end()
            cache = {
                "mode": "naive",
                "same_as_current": False,
                "ticks": shadow.final_tick or shadow.tick,
                "max_state_bytes": max((a.state_bytes() for a in shadow.aggs), default=0),
                "max_traffic": max((a.traffic_in for a in shadow.aggs), default=0),
                "peak_queue": max((h["max_queue"] for h in shadow.history), default=0),
                "history": shadow.history[-400:],
            }
        self._baseline_cache = cache
        return cache

    def results(self) -> dict:
        """Final result per base key: lane states merged by the metric rule."""

        metric = self.settings.metric
        grouped: Dict[str, List[Tuple[str, dict]]] = {}
        for (m, identity), st in self.publisher.state.items():
            if m != metric or not st["rows"]:
                continue
            base = identity.split("#p")[0]
            grouped.setdefault(base, []).append((identity, st))
        out = []
        for base, parts in sorted(grouped.items(), key=lambda kv: -sum(s["rows"] for _, s in kv[1])):
            merged = combine.new_state(metric)
            lane_values = []
            for identity, st in sorted(parts, key=lambda kv: kv[0]):
                combine.merge_state(metric, merged, st)
                lane_values.append({
                    "identity": identity,
                    "value": combine.final_value(metric, st),
                    "rows": st["rows"],
                })
            truth = self.source.truth(base, metric)
            value = combine.final_value(metric, merged)
            out.append({
                "base": base,
                "value": value,
                "truth": truth,
                "error": None if truth is None else round(value - truth, 4),
                "rows": merged["rows"],
                "support": combine.evidence(metric, merged),
                "lanes": lane_values,
                "split_id": (self.split_info.get(base) or {}).get("split_id"),
            })
        out.sort(key=lambda r: -abs(r["error"]) if r["error"] is not None else 0)
        return {
            "metric": metric,
            "info": combine.METRIC_INFO[metric],
            "rows": out,
            "totals": {
                "rows": sum(r["rows"] for r in out),
                "truth_rows": self.source.row_count,
            },
        }

    def base_view(self, base_key: Optional[str] = None) -> dict:
        metric = self.settings.metric
        base_key = base_key or self.settings.hot_key
        info = self.split_info.get(base_key) or {}
        lanes: List[dict] = []
        lane_states: List[dict] = []
        lane_ids = info.get("lanes") or [f"{base_key}#p{i}" for i in range(self.settings.fanout)] \
            if self.settings.mode != "naive" else [base_key]
        for identity in lane_ids:
            local = []
            for agg in self.aggs:
                st = agg.state.get((metric, identity))
                if st is not None and st["rows"]:
                    local.append({
                        "process": agg.name,
                        "rows": st["rows"],
                        "value": combine.final_value(metric, st),
                        "support": combine.evidence(metric, st),
                    })
            published = self.publisher.state.get((metric, identity))
            lane = {
                "identity": identity,
                "local": local,
                "published": None if published is None else {
                    "rows": published["rows"],
                    "value": combine.final_value(metric, published),
                    "support": combine.evidence(metric, published),
                },
            }
            lanes.append(lane)
            if published is not None and published["rows"]:
                lane_states.append(published)
        base_state = self.publisher.state.get((metric, base_key))
        if base_state is not None and base_state["rows"]:
            lane_states.append(base_state)
        merged = combine.new_state(metric)
        for st in lane_states:
            combine.merge_state(metric, merged, st)
        truth = self.source.truth(base_key, metric)
        value = combine.final_value(metric, merged)
        naive = combine.naive_merge(metric, lane_states) if lane_states else None
        return {
            "base": base_key,
            "metric": metric,
            "info": combine.METRIC_INFO[metric],
            "split": info or None,
            "lanes": lanes,
            "merged": {
                "value": value,
                "rows": merged["rows"],
                "support": combine.evidence(metric, merged),
            },
            "truth": truth,
            "error": None if truth is None else round(value - truth, 4),
            "naive": None if naive is None else {
                **naive,
                "error": None if truth is None else round(naive["value"] - truth, 4),
            },
            "baseline": self._baseline_summary(),
        }


# ``traffic`` is derived, but the distributor still counts what it admitted.
DistributionService.traffic = 0  # type: ignore[attr-defined]
