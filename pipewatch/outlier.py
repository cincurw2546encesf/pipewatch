"""Outlier detection: flag pipelines whose recent duration deviates
strongly from their historical mean."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional
import math

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class OutlierResult:
    pipeline: str
    mean_seconds: Optional[float]
    stddev_seconds: Optional[float]
    last_duration_seconds: Optional[float]
    z_score: Optional[float]
    is_outlier: bool
    threshold: float

    def summary(self) -> str:
        if self.last_duration_seconds is None:
            return f"{self.pipeline}: no recent duration data"
        if self.mean_seconds is None:
            return f"{self.pipeline}: insufficient history for outlier detection"
        flag = " [OUTLIER]" if self.is_outlier else ""
        return (
            f"{self.pipeline}: duration={self.last_duration_seconds:.1f}s "
            f"mean={self.mean_seconds:.1f}s stddev={self.stddev_seconds:.1f}s "
            f"z={self.z_score:.2f}{flag}"
        )


def _compute_stats(durations: List[float]):
    """Return (mean, stddev) or (None, None) if fewer than 2 samples."""
    n = len(durations)
    if n < 2:
        return None, None
    mean = sum(durations) / n
    variance = sum((x - mean) ** 2 for x in durations) / (n - 1)
    return mean, math.sqrt(variance)


def check_outlier(
    pipeline: PipelineConfig,
    store: HistoryStore,
    threshold: float = 3.0,
    window: int = 30,
    _now: Callable[[], datetime] = _utcnow,
) -> OutlierResult:
    """Check whether the most recent run duration is an outlier (|z| > threshold)."""
    entries: List[HistoryEntry] = store.get(pipeline.name, limit=window)

    durations = [
        e.duration_seconds
        for e in entries
        if e.duration_seconds is not None
    ]

    last_duration = durations[0] if durations else None
    mean, stddev = _compute_stats(durations)

    z_score: Optional[float] = None
    is_outlier = False

    if mean is not None and stddev is not None and last_duration is not None:
        if stddev > 0:
            z_score = (last_duration - mean) / stddev
            is_outlier = abs(z_score) > threshold
        else:
            z_score = 0.0

    return OutlierResult(
        pipeline=pipeline.name,
        mean_seconds=mean,
        stddev_seconds=stddev,
        last_duration_seconds=last_duration,
        z_score=z_score,
        is_outlier=is_outlier,
        threshold=threshold,
    )


def check_all_outliers(
    pipelines: List[PipelineConfig],
    store: HistoryStore,
    threshold: float = 3.0,
    window: int = 30,
    _now: Callable[[], datetime] = _utcnow,
) -> List[OutlierResult]:
    return [
        check_outlier(p, store, threshold=threshold, window=window, _now=_now)
        for p in pipelines
    ]
