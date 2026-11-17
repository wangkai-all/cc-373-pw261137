"""Event log and incarnation registry.

In-flight aggregates and the publisher live in one process here, but the
*identity* discipline is what production would need anyway: every partial names
the incarnation and code version that built it, and the publisher accepts only
its own.  A restart therefore cannot resurrect pre-restart results.
"""

from __future__ import annotations

import bisect
import itertools
import threading
import time
from collections import deque
from typing import Deque, List, Optional

_MAX = deque  # type alias marker


class IncarnationRegistry:
    _lock = threading.Lock()
    _counter = itertools.count(1)

    @classmethod
    def next(cls) -> int:
        with cls._lock:
            return next(cls._counter)

    @classmethod
    def current(cls) -> int:
        with cls._lock:
            return next(cls._counter) - 1


class EventLog:
    """Bounded ring of ``(t, kind, message)`` entries for the UI timeline."""

    def __init__(self, limit: int = 400) -> None:
        self.limit = limit
        self._entries: Deque[dict] = deque(maxlen=limit)
        self._seq = itertools.count(1)

    def add(self, kind: str, action: str, message: str,
            t: Optional[float] = None) -> dict:
        entry = {
            "seq": next(self._seq),
            "t": round(t if t is not None else time.monotonic() % 100000, 3),
            "kind": kind,
            "action": action,
            "message": message,
        }
        self._entries.append(entry)
        return entry

    def tail(self, n: int = 80) -> List[dict]:
        return list(self._entries)[-n:]
