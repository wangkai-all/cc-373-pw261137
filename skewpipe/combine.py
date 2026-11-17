"""Per-metric local state, deltas, and merge rules.

The central idea: **not every metric combines by addition**.

========  ==========================  ==============================
metric    evidence a partial keeps    merge rule
========  ==========================  ==============================
count     n                           n1 + n2
sum       n, sum                      (n1+n2, sum1+sum2)
avg       sum, cnt                    (sum1+sum2) / (cnt1+cnt2)
distinct  the member set              |union(members)|
========  ==========================  ==============================

For ``avg`` we keep the numerator *and* denominator, so a partial mean can be
corrected later (late rows just add to both).  For ``distinct`` we keep the
members, so a merge is a union -- adding the per-lane distinct counts would
double-count members that appear in more than one lane.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, List, Optional, Set

MASK64 = (1 << 64) - 1

METRIC_ORDER = ("count", "sum", "avg", "distinct")

METRIC_INFO: Dict[str, dict] = {
    "count": {
        "label": "计数 count",
        "evidence": "计数器 n",
        "merge": "直接相加",
        "additive": True,
    },
    "sum": {
        "label": "求和 sum",
        "evidence": "n 与 sum",
        "merge": "直接相加",
        "additive": True,
    },
    "avg": {
        "label": "均值 avg",
        "evidence": "分子 sum 与分母 cnt（成对保留）",
        "merge": "Σsum / Σcnt（按 cnt 加权）",
        "additive": False,
    },
    "distinct": {
        "label": "去重数 distinct",
        "evidence": "成员集合（可并集的证据）",
        "merge": "成员求并集后取基数",
        "additive": False,
    },
}


def new_state(metric: str) -> dict:
    return {"rows": 0, **inner(metric)}


def inner(metric: str) -> dict:
    if metric == "count":
        return {"n": 0}
    if metric == "sum":
        return {"sum": 0}
    if metric == "avg":
        return {"sum": 0, "cnt": 0}
    if metric == "distinct":
        return {"members": set()}
    raise ValueError(f"unknown metric {metric!r}")


def new_delta(metric: str) -> dict:
    """A fresh delta.  Distinct keeps a *list* (append-only evidence per tick)."""

    if metric == "distinct":
        return {"rows": 0, "members": []}
    return {"rows": 0, **inner(metric)}


def apply_event(metric: str, state: dict, event: dict) -> List[str]:
    """Fold one source row into local state.

    Returns the members newly added to a distinct set, so the caller can emit
    only genuinely new evidence in the next delta.
    """

    state["rows"] += 1
    if metric == "count":
        state["n"] += 1
    elif metric == "sum":
        state["sum"] += int(event["amount"])
    elif metric == "avg":
        state["sum"] += int(event["amount"])
        state["cnt"] += 1
    elif metric == "distinct":
        member = event["member"]
        if member in state["members"]:
            return []
        state["members"].add(member)
        return [member]
    else:
        raise ValueError(f"unknown metric {metric!r}")
    return []


def delta_add(metric: str, delta: dict, event: dict, new_members: List[str]) -> None:
    delta["rows"] += 1
    if metric == "count":
        delta["n"] += 1
    elif metric == "sum":
        delta["sum"] += int(event["amount"])
    elif metric == "avg":
        delta["sum"] += int(event["amount"])
        delta["cnt"] += 1
    elif metric == "distinct":
        delta["members"].extend(new_members)
    else:
        raise ValueError(f"unknown metric {metric!r}")


def delta_empty(metric: str, delta: dict) -> bool:
    if delta["rows"] == 0:
        return True
    if metric == "distinct":
        return not delta["members"]
    return True if delta["rows"] == 0 else False


def merge_delta(metric: str, state: dict, delta: dict) -> None:
    """Publisher-side merge of one partial (delta) into accumulated state."""

    state["rows"] += int(delta.get("rows", 0))
    if metric == "count":
        state["n"] += int(delta["n"])
    elif metric == "sum":
        state["sum"] += int(delta["sum"])
    elif metric == "avg":
        state["sum"] += int(delta["sum"])
        state["cnt"] += int(delta["cnt"])
    elif metric == "distinct":
        state["members"].update(delta["members"])
    else:
        raise ValueError(f"unknown metric {metric!r}")


def merge_state(metric: str, dst: dict, src: dict) -> None:
    """Merge two *merged* states (used for carry-forward and logical views)."""

    dst["rows"] += src["rows"]
    if metric == "count":
        dst["n"] += src["n"]
    elif metric == "sum":
        dst["sum"] += src["sum"]
    elif metric == "avg":
        dst["sum"] += src["sum"]
        dst["cnt"] += src["cnt"]
    elif metric == "distinct":
        dst["members"].update(src["members"])
    else:
        raise ValueError(f"unknown metric {metric!r}")


def copy_state(metric: str, state: dict) -> dict:
    out = {k: v for k, v in state.items() if k != "members"}
    if metric == "distinct":
        out["members"] = set(state["members"])
    return out


def final_value(metric: str, state: dict) -> Any:
    if metric == "count":
        return state["n"]
    if metric == "sum":
        return state["sum"]
    if metric == "avg":
        return round(state["sum"] / state["cnt"], 4) if state["cnt"] else 0.0
    if metric == "distinct":
        return len(state["members"])
    raise ValueError(f"unknown metric {metric!r}")


def evidence(metric: str, state: dict) -> dict:
    """A JSON-friendly description of what a partial carries."""

    if metric == "count":
        return {"kind": "counter", "n": state["n"], "rows": state["rows"]}
    if metric == "sum":
        return {"kind": "sum", "sum": state["sum"], "rows": state["rows"]}
    if metric == "avg":
        return {
            "kind": "fraction",
            "sum": state["sum"],
            "cnt": state["cnt"],
            "rows": state["rows"],
            "mean": round(state["sum"] / state["cnt"], 4) if state["cnt"] else 0.0,
        }
    return {"kind": "member-set", "members": len(state["members"]), "rows": state["rows"]}


def state_bytes(metric: str, state: dict) -> int:
    """Rough in-memory footprint of retained evidence (illustrative)."""

    if metric == "count":
        return 32
    if metric == "sum":
        return 40
    if metric == "avg":
        return 56
    if metric == "distinct":
        # 64 B set header + per member: set slot + object + short string
        return 64 + len(state["members"]) * 44
    raise ValueError(f"unknown metric {metric!r}")


def stable_hash(text: str) -> int:
    """Deterministic across processes (built-in hash() is salted)."""

    return int.from_bytes(hashlib.blake2b(text.encode(), digest_size=8).digest(), "big")


def splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & MASK64
    z = x
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
    return (z ^ (z >> 31)) & MASK64


def naive_merge(metric: str, parts: List[dict]) -> Optional[dict]:
    """What "just add the partials" produces, and how wrong it is.

    ``parts`` are merged states for the individual lanes of one key.
    """

    if metric in ("count", "sum"):
        total = sum(final_value(metric, p) for p in parts)
        return {"rule": "直接相加", "value": total, "correct": True}
    if metric == "avg":
        means = [p["sum"] / p["cnt"] for p in parts if p["cnt"]]
        naive = round(sum(means) / len(means), 4) if means else 0.0
        return {"rule": "各分片均值再平均", "value": naive, "correct": False}
    sizes = [len(p["members"]) for p in parts]
    return {"rule": "各分片去重数相加", "value": sum(sizes), "correct": False}
