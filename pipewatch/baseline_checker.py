"""Compare recent run durations against stored baselines."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from pipewatch.baseline import BaselineStore, BaselineEntry
from pipewatch.history import HistoryStore, HistoryEntry


@dataclass
class BaselineCheckResult:
    pipeline: str
    baseline: Optional[BaselineEntry]
    last_duration_seconds: Optional[float]
    deviation_pct: Optional[float]
    exceeded_threshold: bool

    def summary(self) -> str:
        if self.baseline is None:
            return f"{self.pipeline}: no baseline recorded"
        if self.last_duration_seconds is None:
            return f"{self.pipeline}: no recent run to compare"
        sign = "+" if self.deviation_pct >= 0 else ""
        flag = " [SLOW]" if self.exceeded_threshold else ""
        return (
            f"{self.pipeline}: last={self.last_duration_seconds:.1f}s "
            f"avg={self.baseline.avg_duration_seconds:.1f}s "
            f"dev={sign}{self.deviation_pct:.1f}%{flag}"
        )


def check_baseline(
    pipeline: str,
    baseline_store: BaselineStore,
    history_store: HistoryStore,
    threshold_pct: float = 50.0,
) -> BaselineCheckResult:
    baseline = baseline_store.get(pipeline)
    entries: list[HistoryEntry] = history_store.get(pipeline, limit=1)
    last_duration: Optional[float] = None
    if entries:
        e = entries[0]
        if e.started_at and e.finished_at:
            last_duration = (e.finished_at - e.started_at).total_seconds()

    deviation: Optional[float] = None
    exceeded = False
    if baseline and last_duration is not None and baseline.avg_duration_seconds > 0:
        deviation = ((last_duration - baseline.avg_duration_seconds) / baseline.avg_duration_seconds) * 100
        exceeded = deviation > threshold_pct

    return BaselineCheckResult(
        pipeline=pipeline,
        baseline=baseline,
        last_duration_seconds=last_duration,
        deviation_pct=round(deviation, 2) if deviation is not None else None,
        exceeded_threshold=exceeded,
    )
