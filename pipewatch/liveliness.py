"""Liveliness checks: detect pipelines that have stopped reporting entirely."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.history import HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LivelinessResult:
    pipeline: str
    last_seen: Optional[datetime]
    seconds_since: Optional[float]
    max_silence_seconds: float
    exceeded: bool

    def summary(self) -> str:
        if self.last_seen is None:
            return f"{self.pipeline}: never reported"
        age_h = (self.seconds_since or 0) / 3600
        status = "DEAD" if self.exceeded else "alive"
        return (
            f"{self.pipeline}: {status} "
            f"(last seen {age_h:.1f}h ago, "
            f"limit {self.max_silence_seconds / 3600:.1f}h)"
        )


def check_liveliness(
    pipeline_name: str,
    store: HistoryStore,
    max_silence_seconds: float,
    now_fn: Callable[[], datetime] = _utcnow,
) -> Optional[LivelinessResult]:
    """Return a LivelinessResult if a max_silence_seconds limit is configured."""
    if max_silence_seconds <= 0:
        return None

    entries = store.get(pipeline_name)
    if not entries:
        return LivelinessResult(
            pipeline=pipeline_name,
            last_seen=None,
            seconds_since=None,
            max_silence_seconds=max_silence_seconds,
            exceeded=True,
        )

    latest = max(entries, key=lambda e: e.finished_at or e.started_at)
    ts = latest.finished_at or latest.started_at
    now = now_fn()
    delta = (now - ts).total_seconds()
    return LivelinessResult(
        pipeline=pipeline_name,
        last_seen=ts,
        seconds_since=delta,
        max_silence_seconds=max_silence_seconds,
        exceeded=delta > max_silence_seconds,
    )


def check_all_liveliness(
    pipelines,
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[LivelinessResult]:
    """Run liveliness checks for every pipeline that defines max_silence_seconds."""
    results: List[LivelinessResult] = []
    for p in pipelines:
        limit = getattr(p, "max_silence_seconds", 0) or 0
        result = check_liveliness(p.name, store, float(limit), now_fn)
        if result is not None:
            results.append(result)
    return results
