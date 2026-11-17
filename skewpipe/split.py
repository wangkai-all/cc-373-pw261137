"""Split tasks and incarnation-aware result identities.

A split task is a *contract*, not just a routing hint:

* who is allowed to produce the new lanes (incarnation ``inc``, code ``code``),
* what the lanes are (``base#p0 … base#p{f-1}``),
* what evidence was folded into the base key before the split, so the merge
  back can carry it forward without re-reading history,
* and whether it has been superseded.

Anything carrying a different incarnation or a lane set that does not match the
task is rejected by the publisher.  That is what stops an old split task from
polluting a new result after a restart or a re-split.
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional, Set


def digest(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


class SplitTask:
    def __init__(self, split_id: str, base_key: str, inc: int, strategy: str,
                 fanout: int, partitions: Dict[str, dict], reason: str = "auto",
                 epoch: int = 1) -> None:
        self.split_id = split_id
        self.base_key = base_key
        self.inc = inc
        self.strategy = strategy
        self.fanout = max(1, int(fanout))
        self.partitions = partitions  # lane -> {"rows": int, "sum": int}
        self.reason = reason
        self.epoch = epoch
        self.carry: Dict[str, dict] = {}  # metric -> merged state (evidence)
        self.retired = False
        self.created_tick = 0
        self.merged_rows = 0
        # digest binds the lane *set* to the task; a stale task's lanes differ.
        self.lane_digest = digest("|".join(self.lanes()))
        self.tag = digest(f"{inc}:{split_id}:{base_key}:{self.strategy}:{self.lane_digest}")

    def lanes(self) -> List[str]:
        return [f"{self.base_key}#p{i}" for i in range(self.fanout)]

    def lane_identity(self, lane: str) -> str:
        return f"{self.base_key}#p{lane}"

    def expected_identity(self, identity: str) -> bool:
        return identity in self.lanes()

    def stale_for(self, inc: int, code: str, identity: str) -> bool:
        return (self.inc != inc) or (not self.expected_identity(identity))

    def snapshot(self) -> dict:
        return {
            "split_id": self.split_id,
            "base_key": self.base_key,
            "inc": self.inc,
            "strategy": self.strategy,
            "fanout": self.fanout,
            "epoch": self.epoch,
            "reason": self.reason,
            "lanes": self.lanes(),
            "lane_digest": self.lane_digest,
            "partitions": self.partitions,
            "merged_rows": self.merged_rows,
            "retired": self.retired,
        }


class SplitTaskRegistry:
    def __init__(self, inc: int) -> None:
        self.inc = inc
        self.tasks: List[SplitTask] = []
        self.active: Dict[str, str] = {}  # base key -> split id
        self.epoch: Dict[str, int] = {}

    def add(self, task: SplitTask) -> None:
        prev = self.active.get(task.base_key)
        if prev:
            for t in self.tasks:
                if t.split_id == prev:
                    t.retired = True
        self.epoch[task.base_key] = self.epoch.get(task.base_key, 0) + 1
        task.epoch = self.epoch[task.base_key]
        task.created_tick = len(self.tasks)
        self.tasks.append(task)
        self.active[task.base_key] = task.split_id

    def get(self, split_id: str) -> Optional[SplitTask]:
        for t in self.tasks:
            if t.split_id == split_id:
                return t
        return None

    def active_for(self, base_key: str) -> Optional[SplitTask]:
        sid = self.active.get(base_key)
        return self.get(sid) if sid else None

    def all_snapshot(self) -> List[dict]:
        return [t.snapshot() for t in self.tasks]
