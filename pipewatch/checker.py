"""Pipeline health-check logic for pipewatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.state import StateStore


class CheckStatus(str, Enum):
    OK = "ok"
    STALE = "stale"
    FAILED = "failed"
    MISSING = "missing"


@dataclass
class CheckResult:
    pipeline_name: str
    status: CheckStatus
    message: str
    last_run: Optional[datetime] = field(default=None)

    def __str__(self) -> str:  # noqa: D401
        last = self.last_run.isoformat() if self.last_run else "never"
        return f"[{self.status.value.upper()}] {self.pipeline_name}: {self.message} (last_run={last})"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def check_pipeline(
    pipeline: PipelineConfig,
    store: StateStore,
    *,
    now: Optional[datetime] = None,
) -> CheckResult:
    """Evaluate the health of a single pipeline."""
    now = now or _utcnow()
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
            message=f"Last run failed: {record.error or 'unknown error'}.",
            last_run=record.finished_dt or record.started_dt,
        )

    reference_time = record.finished_dt or record.started_dt
    if reference_time is None:
        return CheckResult(
            pipeline_name=pipeline.name,
            status=CheckStatus.MISSING,
            message="Run record has no timestamp.",
        )

    age_seconds = (now - reference_time).total_seconds()
    if age_seconds > pipeline.max_age_seconds:
        return CheckResult(
            pipeline_name=pipeline.name,
            status=CheckStatus.STALE,
            message=(
                f"Last successful run was {age_seconds / 3600:.1f}h ago "
                f"(threshold {pipeline.max_age_seconds / 3600:.1f}h)."
            ),
            last_run=reference_time,
        )

    return CheckResult(
        pipeline_name=pipeline.name,
        status=CheckStatus.OK,
        message="Pipeline is healthy.",
        last_run=reference_time,
    )


def check_all(
    pipelines: List[PipelineConfig],
    store: StateStore,
    *,
    now: Optional[datetime] = None,
) -> List[CheckResult]:
    """Run health checks for every pipeline in *pipelines*."""
    return [check_pipeline(p, store, now=now) for p in pipelines]
