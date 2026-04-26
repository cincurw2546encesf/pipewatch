"""Jitter detection for pipeline runs.

Detects whether a pipeline's run intervals are inconsistent (high variance),
which may indicate scheduling instability even when runs are completing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.config import AppConfig
from pipewatch.history import HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class JitterResult:
    """Result of a jitter check for a single pipeline."""

    pipeline: str
    mean_interval_seconds: Optional[float]
    stddev_seconds: Optional[float]
    cv: Optional[float]  # coefficient of variation = stddev / mean
    threshold_cv: float
    exceeded: bool
    sample_count: int

    def summary(self) -> str:
        if self.mean_interval_seconds is None:
            return f"{self.pipeline}: insufficient history for jitter analysis"
        cv_pct = (self.cv or 0.0) * 100
        mean_min = (self.mean_interval_seconds or 0) / 60
        std_min = (self.stddev_seconds or 0) / 60
        status = "JITTER" if self.exceeded else "OK"
        return (
            f"{self.pipeline}: [{status}] mean={mean_min:.1f}m "
            f"stddev={std_min:.1f}m CV={cv_pct:.1f}% "
            f"(threshold={self.threshold_cv * 100:.0f}%)"
        )


def _compute_stddev(values: List[float]) -> tuple[float, float]:
    """Return (mean, stddev) for a list of floats."""
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return mean, math.sqrt(variance)


def check_jitter(
    pipeline_name: str,
    history: HistoryStore,
    threshold_cv: float = 0.5,
    min_samples: int = 4,
    max_entries: int = 20,
) -> JitterResult:
    """Check whether a pipeline's run intervals show excessive jitter.

    Args:
        pipeline_name: Name of the pipeline to analyse.
        history: HistoryStore to load entries from.
        threshold_cv: Coefficient of variation above which jitter is flagged.
                      Defaults to 0.5 (50%).
        min_samples: Minimum number of interval samples required.
        max_entries: Maximum recent history entries to consider.

    Returns:
        JitterResult with analysis.
    """
    entries = history.get(pipeline_name, limit=max_entries)
    # Only consider entries that have a recorded start time
    timestamps: List[datetime] = sorted(
        (e.started_at for e in entries if e.started_at is not None),
        key=lambda dt: dt,
    )

    if len(timestamps) < min_samples + 1:
        return JitterResult(
            pipeline=pipeline_name,
            mean_interval_seconds=None,
            stddev_seconds=None,
            cv=None,
            threshold_cv=threshold_cv,
            exceeded=False,
            sample_count=max(0, len(timestamps) - 1),
        )

    intervals = [
        (timestamps[i + 1] - timestamps[i]).total_seconds()
        for i in range(len(timestamps) - 1)
    ]

    mean, stddev = _compute_stddev(intervals)
    cv = stddev / mean if mean > 0 else 0.0
    exceeded = cv > threshold_cv

    return JitterResult(
        pipeline=pipeline_name,
        mean_interval_seconds=mean,
        stddev_seconds=stddev,
        cv=cv,
        threshold_cv=threshold_cv,
        exceeded=exceeded,
        sample_count=len(intervals),
    )


def check_all_jitter(
    results: List[CheckResult],
    app_cfg: AppConfig,
    history: HistoryStore,
    threshold_cv: float = 0.5,
    min_samples: int = 4,
) -> List[JitterResult]:
    """Run jitter checks for all pipelines that appear in check results."""
    return [
        check_jitter(
            pipeline_name=r.pipeline,
            history=history,
            threshold_cv=threshold_cv,
            min_samples=min_samples,
        )
        for r in results
    ]
