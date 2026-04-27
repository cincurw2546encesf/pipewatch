"""Velocity checker: detects when a pipeline's run frequency has dropped
below an expected threshold over a rolling window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class VelocityResult:
    pipeline: str
    expected_runs: int
    actual_runs: int
    window_hours: int
    exceeded: bool

    def summary(self) -> str:
        status = "LOW" if self.exceeded else "OK"
        return (
            f"[{status}] {self.pipeline}: {self.actual_runs}/{self.expected_runs} "
            f"runs in last {self.window_hours}h"
        )


def check_velocity(
    pipeline: PipelineConfig,
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> Optional[VelocityResult]:
    """Return a VelocityResult if the pipeline has velocity config, else None."""
    window_hours: Optional[int] = getattr(pipeline, "velocity_window_hours", None)
    expected_runs: Optional[int] = getattr(pipeline, "velocity_min_runs", None)

    if window_hours is None or expected_runs is None:
        return None

    now = now_fn()
    cutoff = now.timestamp() - window_hours * 3600

    entries: List[HistoryEntry] = store.get(pipeline.name)
    recent = [
        e for e in entries
        if e.checked_at is not None and e.checked_at.timestamp() >= cutoff
    ]
    actual = len(recent)
    exceeded = actual < expected_runs

    return VelocityResult(
        pipeline=pipeline.name,
        expected_runs=expected_runs,
        actual_runs=actual,
        window_hours=window_hours,
        exceeded=exceeded,
    )


def check_all_velocity(
    pipelines: List[PipelineConfig],
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[VelocityResult]:
    results = []
    for p in pipelines:
        r = check_velocity(p, store, now_fn=now_fn)
        if r is not None:
            results.append(r)
    return results
