"""Pipeline health checker."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Callable, List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.state import StateStore


class CheckStatus(str, Enum):
    OK = "ok"
    STALE = "stale"
    FAILED = "failed"
    MISSING = "missing"


class CheckResult:
    def __init__(
        self,
        pipeline_name: str,
        status: CheckStatus,
        message: str,
        last_run_at: Optional[datetime] = None,
        age_minutes: Optional[float] = None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.status = status
        self.message = message
        self.last_run_at = last_run_at
        self.age_minutes = age_minutes

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"[{self.status.value.upper()}] {self.pipeline_name}: {self.message}"
        )


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def check_pipeline(
    pipeline: PipelineConfig,
    store: StateStore,
    *,
    _now_fn: Optional[Callable] = None,
) -> CheckResult:
    """Check a single pipeline and return a :class:`CheckResult`."""
    now = (_now_fn or _utcnow)()
    record = store.latest(pipeline.name)

    if record is None:
        return CheckResult(
            pipeline_name=pipeline.name,
            status=CheckStatus.MISSING,
            message="No run record found.",
        )

    if record.status == "failed":
        return CheckResult(
            pipeline_name=pipeline.name,
            status=CheckStatus.FAILED,
            message=f"Last run failed (run_id={record.run_id}).",
            last_run_at=record.finished_at,
        )

    finished = record.finished_at
    if finished is None:
        return CheckResult(
            pipeline_name=pipeline.name,
            status=CheckStatus.FAILED,
            message="Last run has no finish timestamp (still running?).",
            last_run_at=record.started_at,
        )

    age_minutes = (now - finished).total_seconds() / 60.0
    if age_minutes > pipeline.max_age_minutes:
        return CheckResult(
            pipeline_name=pipeline.name,
            status=CheckStatus.STALE,
            message=(
                f"Last successful run finished {age_minutes:.1f} min ago "
                f"(threshold={pipeline.max_age_minutes} min)."
            ),
            last_run_at=finished,
            age_minutes=age_minutes,
        )

    return CheckResult(
        pipeline_name=pipeline.name,
        status=CheckStatus.OK,
        message=f"Healthy — last run {age_minutes:.1f} min ago.",
        last_run_at=finished,
        age_minutes=age_minutes,
    )


def check_all(
    pipelines: List[PipelineConfig],
    store: StateStore,
    *,
    _now_fn: Optional[Callable] = None,
) -> List[CheckResult]:
    """Run :func:`check_pipeline` for every pipeline in *pipelines*."""
    return [check_pipeline(p, store, _now_fn=_now_fn) for p in pipelines]
