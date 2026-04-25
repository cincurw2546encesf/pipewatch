"""Drift detection: flag pipelines whose run interval has changed significantly."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DriftResult:
    pipeline: str
    expected_interval_seconds: float
    actual_interval_seconds: Optional[float]
    drift_ratio: Optional[float]  # actual / expected
    exceeded: bool
    message: str

    def summary(self) -> str:
        if self.actual_interval_seconds is None:
            return f"{self.pipeline}: no interval data"
        pct = (self.drift_ratio - 1.0) * 100 if self.drift_ratio is not None else 0
        direction = "slower" if pct > 0 else "faster"
        return (
            f"{self.pipeline}: {abs(pct):.1f}% {direction} than expected "
            f"(expected {self.expected_interval_seconds:.0f}s, "
            f"actual {self.actual_interval_seconds:.0f}s)"
        )


def _mean_interval(timestamps: List[datetime]) -> Optional[float]:
    """Return mean gap in seconds between consecutive timestamps."""
    if len(timestamps) < 2:
        return None
    sorted_ts = sorted(timestamps)
    gaps = [
        (sorted_ts[i + 1] - sorted_ts[i]).total_seconds()
        for i in range(len(sorted_ts) - 1)
    ]
    return sum(gaps) / len(gaps)


def check_drift(
    pipeline: str,
    expected_interval_seconds: float,
    store: HistoryStore,
    tolerance: float = 0.5,
    min_entries: int = 3,
) -> DriftResult:
    """Check whether the pipeline's actual run cadence has drifted beyond tolerance."""
    entries = store.get(pipeline)
    timestamps = [
        e.checked_at for e in entries if e.checked_at is not None
    ]

    if len(timestamps) < min_entries:
        return DriftResult(
            pipeline=pipeline,
            expected_interval_seconds=expected_interval_seconds,
            actual_interval_seconds=None,
            drift_ratio=None,
            exceeded=False,
            message=f"insufficient history ({len(timestamps)} entries, need {min_entries})",
        )

    actual = _mean_interval(timestamps)
    if actual is None or expected_interval_seconds <= 0:
        return DriftResult(
            pipeline=pipeline,
            expected_interval_seconds=expected_interval_seconds,
            actual_interval_seconds=actual,
            drift_ratio=None,
            exceeded=False,
            message="unable to compute drift",
        )

    ratio = actual / expected_interval_seconds
    exceeded = abs(ratio - 1.0) > tolerance
    return DriftResult(
        pipeline=pipeline,
        expected_interval_seconds=expected_interval_seconds,
        actual_interval_seconds=actual,
        drift_ratio=ratio,
        exceeded=exceeded,
        message="drift detected" if exceeded else "within tolerance",
    )


def check_all_drift(
    pipelines: dict,
    store: HistoryStore,
    tolerance: float = 0.5,
) -> List[DriftResult]:
    """Run drift check for all pipelines that define expected_interval_seconds."""
    results = []
    for name, cfg in pipelines.items():
        interval = getattr(cfg, "expected_interval_seconds", None)
        if interval is None:
            continue
        results.append(check_drift(name, interval, store, tolerance=tolerance))
    return results
