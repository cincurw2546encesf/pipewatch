"""Freshness checker: verifies pipelines ran within their expected cadence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import PipelineConfig
from pipewatch.state import StateStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FreshnessResult:
    pipeline: str
    expected_cadence_seconds: int
    seconds_since_last_run: Optional[float]
    exceeded: bool
    last_run: Optional[datetime]

    def summary(self) -> str:
        if self.seconds_since_last_run is None:
            return f"{self.pipeline}: never run (cadence {self.expected_cadence_seconds}s)"
        status = "EXCEEDED" if self.exceeded else "OK"
        return (
            f"{self.pipeline}: {status} — "
            f"{self.seconds_since_last_run:.0f}s since last run "
            f"(cadence {self.expected_cadence_seconds}s)"
        )


def check_freshness(
    pipeline: PipelineConfig,
    store: StateStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> Optional[FreshnessResult]:
    """Return a FreshnessResult if the pipeline has a max_age_seconds configured."""
    cadence = getattr(pipeline, "max_age_seconds", None)
    if cadence is None:
        return None

    record = store.latest(pipeline.name)
    now = now_fn()

    if record is None or record.finished_at is None:
        return FreshnessResult(
            pipeline=pipeline.name,
            expected_cadence_seconds=cadence,
            seconds_since_last_run=None,
            exceeded=True,
            last_run=None,
        )

    last_run = record.finished_at
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    elapsed = (now - last_run).total_seconds()
    return FreshnessResult(
        pipeline=pipeline.name,
        expected_cadence_seconds=cadence,
        seconds_since_last_run=elapsed,
        exceeded=elapsed > cadence,
        last_run=last_run,
    )


def check_all_freshness(
    pipelines: List[PipelineConfig],
    store: StateStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[FreshnessResult]:
    results = []
    for p in pipelines:
        r = check_freshness(p, store, now_fn=now_fn)
        if r is not None:
            results.append(r)
    return results
