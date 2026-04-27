"""Capacity tracking: detect when a pipeline's run duration is trending toward its time budget."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.config import PipelineConfig


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CapacityResult:
    pipeline: str
    avg_duration_seconds: Optional[float]
    budget_seconds: Optional[float]
    utilisation: Optional[float]   # 0.0 – 1.0+
    exceeded: bool
    sample_count: int

    def summary(self) -> str:
        if self.avg_duration_seconds is None:
            return f"{self.pipeline}: no duration data"
        if self.budget_seconds is None:
            return f"{self.pipeline}: no budget configured"
        pct = (self.utilisation or 0.0) * 100
        status = "EXCEEDED" if self.exceeded else "ok"
        return (
            f"{self.pipeline}: avg {self.avg_duration_seconds:.1f}s / "
            f"budget {self.budget_seconds:.1f}s ({pct:.1f}%) [{status}]"
        )


def check_capacity(
    pipeline: PipelineConfig,
    store: HistoryStore,
    window: int = 10,
) -> CapacityResult:
    """Compute average duration over the last *window* runs and compare to budget."""
    entries: List[HistoryEntry] = store.get(pipeline.name, limit=window)

    durations = [
        e.duration_seconds
        for e in entries
        if e.duration_seconds is not None
    ]

    if not durations:
        return CapacityResult(
            pipeline=pipeline.name,
            avg_duration_seconds=None,
            budget_seconds=getattr(pipeline, "budget_seconds", None),
            utilisation=None,
            exceeded=False,
            sample_count=0,
        )

    avg = sum(durations) / len(durations)
    budget: Optional[float] = getattr(pipeline, "budget_seconds", None)

    utilisation: Optional[float] = (avg / budget) if budget else None
    exceeded = bool(utilisation is not None and utilisation > 1.0)

    return CapacityResult(
        pipeline=pipeline.name,
        avg_duration_seconds=avg,
        budget_seconds=budget,
        utilisation=utilisation,
        exceeded=exceeded,
        sample_count=len(durations),
    )


def check_all_capacity(
    pipelines: List[PipelineConfig],
    store: HistoryStore,
    window: int = 10,
) -> List[CapacityResult]:
    return [check_capacity(p, store, window=window) for p in pipelines]
