"""drain.py — detect pipelines that are consistently running but producing no output.

A pipeline is considered 'draining' if it has run successfully multiple times
recently but all runs completed in under a minimum expected duration, suggesting
it may be processing empty batches.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DrainResult:
    pipeline: str
    run_count: int
    avg_duration_seconds: Optional[float]
    min_expected_seconds: Optional[float]
    is_draining: bool

    def summary(self) -> str:
        if self.avg_duration_seconds is None:
            return f"{self.pipeline}: no duration data"
        if self.is_draining:
            return (
                f"{self.pipeline}: draining — avg {self.avg_duration_seconds:.1f}s "
                f"< min expected {self.min_expected_seconds:.1f}s over {self.run_count} runs"
            )
        return (
            f"{self.pipeline}: ok — avg {self.avg_duration_seconds:.1f}s "
            f"over {self.run_count} runs"
        )


def check_drain(
    pipeline,
    store: HistoryStore,
    window: int = 10,
    now_fn=None,
) -> Optional[DrainResult]:
    """Check whether a pipeline appears to be draining (empty runs)."""
    min_expected = getattr(pipeline, "min_duration_seconds", None)
    if min_expected is None:
        return None

    entries = store.get(pipeline.name)
    if not entries:
        return DrainResult(
            pipeline=pipeline.name,
            run_count=0,
            avg_duration_seconds=None,
            min_expected_seconds=min_expected,
            is_draining=False,
        )

    recent = [e for e in entries if e.duration_seconds is not None][-window:]
    if not recent:
        return DrainResult(
            pipeline=pipeline.name,
            run_count=0,
            avg_duration_seconds=None,
            min_expected_seconds=min_expected,
            is_draining=False,
        )

    avg = sum(e.duration_seconds for e in recent) / len(recent)
    is_draining = avg < min_expected

    return DrainResult(
        pipeline=pipeline.name,
        run_count=len(recent),
        avg_duration_seconds=avg,
        min_expected_seconds=min_expected,
        is_draining=is_draining,
    )


def check_all_drain(pipelines: list, store: HistoryStore, window: int = 10) -> List[DrainResult]:
    results = []
    for p in pipelines:
        r = check_drain(p, store, window=window)
        if r is not None:
            results.append(r)
    return results
