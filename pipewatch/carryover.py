"""Carryover: detect pipelines whose last run started but never finished."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.state import StateStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CarryoverResult:
    pipeline: str
    started_at: Optional[datetime]
    carried_over_seconds: Optional[float]
    exceeded: bool
    max_carryover_seconds: Optional[int]

    def summary(self) -> str:
        if self.started_at is None:
            return f"{self.pipeline}: no active run"
        if not self.exceeded:
            secs = int(self.carried_over_seconds or 0)
            return f"{self.pipeline}: running for {secs}s (within limit)"
        secs = int(self.carried_over_seconds or 0)
        return f"{self.pipeline}: STUCK — running for {secs}s (limit {self.max_carryover_seconds}s)"


def check_carryover(
    pipeline: PipelineConfig,
    store: StateStore,
    now_fn=_utcnow,
) -> Optional[CarryoverResult]:
    max_secs = getattr(pipeline, "max_carryover_seconds", None)
    record = store.latest(pipeline.name)

    # Only flag if a run has started but not finished
    if record is None or record.started_dt is None:
        return None
    if record.finished_dt is not None:
        return None  # run completed normally

    now = now_fn()
    elapsed = (now - record.started_dt).total_seconds()

    if max_secs is None:
        return CarryoverResult(
            pipeline=pipeline.name,
            started_at=record.started_dt,
            carried_over_seconds=elapsed,
            exceeded=False,
            max_carryover_seconds=None,
        )

    return CarryoverResult(
        pipeline=pipeline.name,
        started_at=record.started_dt,
        carried_over_seconds=elapsed,
        exceeded=elapsed > max_secs,
        max_carryover_seconds=max_secs,
    )


def check_all_carryover(
    app_cfg: AppConfig,
    store: StateStore,
    now_fn=_utcnow,
) -> List[CarryoverResult]:
    results = []
    for pipeline in app_cfg.pipelines:
        result = check_carryover(pipeline, store, now_fn=now_fn)
        if result is not None:
            results.append(result)
    return results
