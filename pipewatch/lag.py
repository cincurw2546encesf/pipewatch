"""Lag tracking: measures how far behind a pipeline is relative to its expected cadence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pipewatch.checker import CheckResult, CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LagResult:
    pipeline: str
    expected_interval_seconds: int
    last_run: Optional[datetime]
    lag_seconds: Optional[float]
    exceeded: bool

    @property
    def summary(self) -> str:
        if self.last_run is None:
            return f"{self.pipeline}: never run (lag unknown)"
        mins = (self.lag_seconds or 0) / 60
        status = "EXCEEDED" if self.exceeded else "ok"
        return (
            f"{self.pipeline}: lag={mins:.1f}m "
            f"(limit={self.expected_interval_seconds // 60}m) [{status}]"
        )


def compute_lag(
    result: CheckResult,
    *,
    now_fn=_utcnow,
) -> LagResult:
    """Compute how many seconds behind the pipeline is vs its expected interval."""
    interval = result.pipeline.max_age_seconds
    last_run = result.last_run

    if last_run is None:
        return LagResult(
            pipeline=result.pipeline.name,
            expected_interval_seconds=interval,
            last_run=None,
            lag_seconds=None,
            exceeded=result.status != CheckStatus.OK,
        )

    now = now_fn()
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    lag = (now - last_run).total_seconds()
    exceeded = lag > interval

    return LagResult(
        pipeline=result.pipeline.name,
        expected_interval_seconds=interval,
        last_run=last_run,
        lag_seconds=lag,
        exceeded=exceeded,
    )


def check_all_lag(results: list[CheckResult], *, now_fn=_utcnow) -> list[LagResult]:
    """Return lag results for all pipelines."""
    return [compute_lag(r, now_fn=now_fn) for r in results]


def lag_summary(lag_results: list[LagResult]) -> str:
    """Build a human-readable summary of lag across all pipelines."""
    exceeded = [r for r in lag_results if r.exceeded]
    lines = [f"Lag report ({len(lag_results)} pipelines, {len(exceeded)} exceeded):"]
    for r in lag_results:
        lines.append(f"  {r.summary}")
    return "\n".join(lines)
