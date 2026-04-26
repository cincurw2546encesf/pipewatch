"""Stagger detection: identify pipelines that consistently run at irregular
intervals compared to their configured schedule frequency."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StaggerResult:
    pipeline: str
    expected_interval_seconds: Optional[float]
    actual_interval_seconds: Optional[float]
    deviation_seconds: Optional[float]
    exceeded: bool
    reason: str

    def summary(self) -> str:
        if not self.exceeded:
            return f"{self.pipeline}: on schedule"
        return (
            f"{self.pipeline}: staggered by {self.deviation_seconds:.0f}s "
            f"(expected ~{self.expected_interval_seconds:.0f}s, "
            f"actual ~{self.actual_interval_seconds:.0f}s)"
        )


def _mean_interval(timestamps: List[datetime]) -> Optional[float]:
    """Return mean interval in seconds between consecutive timestamps."""
    if len(timestamps) < 2:
        return None
    sorted_ts = sorted(timestamps)
    gaps = [
        (sorted_ts[i + 1] - sorted_ts[i]).total_seconds()
        for i in range(len(sorted_ts) - 1)
    ]
    return sum(gaps) / len(gaps)


def check_stagger(
    pipeline: PipelineConfig,
    store: HistoryStore,
    tolerance: float = 0.25,
    min_entries: int = 3,
) -> StaggerResult:
    """Check whether a pipeline's run cadence deviates from its expected interval.

    Args:
        pipeline: Pipeline configuration (must have max_age_seconds set).
        store: History store to pull past run timestamps from.
        tolerance: Fractional tolerance before flagging (default 25%).
        min_entries: Minimum history entries required to evaluate.
    """
    entries = store.get(pipeline.name)
    if pipeline.max_age_seconds is None:
        return StaggerResult(
            pipeline=pipeline.name,
            expected_interval_seconds=None,
            actual_interval_seconds=None,
            deviation_seconds=None,
            exceeded=False,
            reason="no max_age_seconds configured",
        )

    if len(entries) < min_entries:
        return StaggerResult(
            pipeline=pipeline.name,
            expected_interval_seconds=pipeline.max_age_seconds,
            actual_interval_seconds=None,
            deviation_seconds=None,
            exceeded=False,
            reason=f"insufficient history ({len(entries)} < {min_entries})",
        )

    timestamps = [e.checked_at for e in entries if e.checked_at is not None]
    actual = _mean_interval(timestamps)
    if actual is None:
        return StaggerResult(
            pipeline=pipeline.name,
            expected_interval_seconds=pipeline.max_age_seconds,
            actual_interval_seconds=None,
            deviation_seconds=None,
            exceeded=False,
            reason="could not compute mean interval",
        )

    expected = pipeline.max_age_seconds
    deviation = abs(actual - expected)
    threshold = expected * tolerance
    exceeded = deviation > threshold

    return StaggerResult(
        pipeline=pipeline.name,
        expected_interval_seconds=expected,
        actual_interval_seconds=actual,
        deviation_seconds=deviation,
        exceeded=exceeded,
        reason="deviation exceeds tolerance" if exceeded else "within tolerance",
    )


def check_all_stagger(
    pipelines: List[PipelineConfig],
    store: HistoryStore,
    tolerance: float = 0.25,
    min_entries: int = 3,
) -> List[StaggerResult]:
    return [
        check_stagger(p, store, tolerance=tolerance, min_entries=min_entries)
        for p in pipelines
    ]
