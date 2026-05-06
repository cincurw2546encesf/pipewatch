"""Census module: counts and categorises pipeline run statuses over a time window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.checker import CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CensusResult:
    pipeline: str
    window_seconds: int
    total: int
    ok: int
    stale: int
    failed: int
    unknown: int

    @property
    def summary(self) -> str:
        if self.total == 0:
            return f"{self.pipeline}: no runs in window"
        return (
            f"{self.pipeline}: {self.total} runs "
            f"[ok={self.ok} stale={self.stale} failed={self.failed} unknown={self.unknown}]"
        )


def check_census(
    pipeline_name: str,
    window_seconds: int,
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> CensusResult:
    """Count run statuses within the last *window_seconds* for *pipeline_name*."""
    now = now_fn()
    entries: List[HistoryEntry] = store.get(pipeline_name)

    counts = {CheckStatus.OK: 0, CheckStatus.STALE: 0, CheckStatus.FAILED: 0}
    unknown = 0

    for entry in entries:
        ts = entry.checked_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (now - ts).total_seconds()
        if age > window_seconds:
            continue
        if entry.status in counts:
            counts[entry.status] += 1
        else:
            unknown += 1

    total = sum(counts.values()) + unknown
    return CensusResult(
        pipeline=pipeline_name,
        window_seconds=window_seconds,
        total=total,
        ok=counts[CheckStatus.OK],
        stale=counts[CheckStatus.STALE],
        failed=counts[CheckStatus.FAILED],
        unknown=unknown,
    )


def check_all_census(
    pipelines: List[str],
    window_seconds: int,
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[CensusResult]:
    return [check_census(p, window_seconds, store, now_fn) for p in pipelines]
