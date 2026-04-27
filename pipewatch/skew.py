"""Skew detection: flag pipelines whose actual run time deviates
significantly from their scheduled/expected start time."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SkewResult:
    pipeline: str
    expected_hour: Optional[int]        # hour-of-day the pipeline should start
    actual_avg_hour: Optional[float]    # observed average start hour
    skew_minutes: Optional[float]       # |expected - actual| in minutes
    exceeded: bool
    max_skew_minutes: Optional[float]

    def summary(self) -> str:
        if self.expected_hour is None:
            return f"{self.pipeline}: no expected_hour configured"
        if self.actual_avg_hour is None:
            return f"{self.pipeline}: insufficient history"
        flag = "SKEWED" if self.exceeded else "OK"
        return (
            f"{self.pipeline}: [{flag}] skew={self.skew_minutes:.1f}m "
            f"(expected={self.expected_hour:02d}:00, "
            f"avg_actual={self.actual_avg_hour:.2f}h, "
            f"max={self.max_skew_minutes}m)"
        )


def check_skew(
    pipeline: PipelineConfig,
    store: HistoryStore,
    min_entries: int = 3,
    now_fn: Callable[[], datetime] = _utcnow,
) -> Optional[SkewResult]:
    expected_hour: Optional[int] = getattr(pipeline, "expected_hour", None)
    max_skew: Optional[float] = getattr(pipeline, "max_skew_minutes", None)

    if expected_hour is None or max_skew is None:
        return None

    entries: List[HistoryEntry] = store.get(pipeline.name)
    starts = [
        e.started_at
        for e in entries
        if e.started_at is not None
    ]

    if len(starts) < min_entries:
        return SkewResult(
            pipeline=pipeline.name,
            expected_hour=expected_hour,
            actual_avg_hour=None,
            skew_minutes=None,
            exceeded=False,
            max_skew_minutes=max_skew,
        )

    avg_hour = sum(
        dt.hour + dt.minute / 60.0 + dt.second / 3600.0 for dt in starts
    ) / len(starts)

    skew_minutes = abs(expected_hour - avg_hour) * 60.0
    exceeded = skew_minutes > max_skew

    return SkewResult(
        pipeline=pipeline.name,
        expected_hour=expected_hour,
        actual_avg_hour=avg_hour,
        skew_minutes=skew_minutes,
        exceeded=exceeded,
        max_skew_minutes=max_skew,
    )


def check_all_skew(
    pipelines: List[PipelineConfig],
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[SkewResult]:
    results = []
    for p in pipelines:
        r = check_skew(p, store, now_fn=now_fn)
        if r is not None:
            results.append(r)
    return results
