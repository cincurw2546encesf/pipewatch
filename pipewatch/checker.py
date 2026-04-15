"""Staleness and failure checker — evaluates pipeline health against config thresholds."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.state import RunRecord, StateStore


@dataclass
class CheckResult:
    pipeline: str
    healthy: bool
    reason: Optional[str] = None
    latest_run: Optional[RunRecord] = None

    def __str__(self) -> str:
        icon = "✅" if self.healthy else "❌"
        base = f"{icon} {self.pipeline}"
        if self.reason:
            base += f" — {self.reason}"
        if self.latest_run:
            base += f" (last run: {self.latest_run.started_at})"
        return base


def check_pipeline(config: PipelineConfig, store: StateStore) -> CheckResult:
    """Return a CheckResult for a single pipeline based on its latest run."""
    latest = store.latest(config.name)

    if latest is None:
        return CheckResult(
            pipeline=config.name,
            healthy=False,
            reason="no runs recorded",
        )

    if latest.status == "failed":
        return CheckResult(
            pipeline=config.name,
            healthy=False,
            reason=f"last run failed: {latest.message or 'no details'}",
            latest_run=latest,
        )

    now = datetime.now(timezone.utc)
    started = latest.started_dt
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)

    age_minutes = (now - started).total_seconds() / 60
    threshold = config.max_age_minutes

    if age_minutes > threshold:
        return CheckResult(
            pipeline=config.name,
            healthy=False,
            reason=f"stale: last run {age_minutes:.1f}m ago (threshold {threshold}m)",
            latest_run=latest,
        )

    return CheckResult(pipeline=config.name, healthy=True, latest_run=latest)


def check_all(configs: List[PipelineConfig], store: StateStore) -> List[CheckResult]:
    """Run health checks for every pipeline in the config list."""
    return [check_pipeline(cfg, store) for cfg in configs]
