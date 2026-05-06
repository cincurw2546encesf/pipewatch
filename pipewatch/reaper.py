"""Reaper: detect and flag pipelines that have not run within a configured expiry window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import AppConfig
from pipewatch.state import StateStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ReaperResult:
    pipeline: str
    expired: bool
    last_run: Optional[datetime]
    expiry_seconds: int
    seconds_since: Optional[float]

    def summary(self) -> str:
        if self.last_run is None:
            return f"{self.pipeline}: never run (expiry {self.expiry_seconds}s)"
        if self.expired:
            return (
                f"{self.pipeline}: expired — last run {self.seconds_since:.0f}s ago "
                f"(limit {self.expiry_seconds}s)"
            )
        return (
            f"{self.pipeline}: ok — last run {self.seconds_since:.0f}s ago "
            f"(limit {self.expiry_seconds}s)"
        )


def check_reaper(
    pipeline_name: str,
    expiry_seconds: int,
    store: StateStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> ReaperResult:
    record = store.latest(pipeline_name)
    if record is None:
        return ReaperResult(
            pipeline=pipeline_name,
            expired=True,
            last_run=None,
            expiry_seconds=expiry_seconds,
            seconds_since=None,
        )
    last_run = record.finished_dt() or record.started_dt()
    if last_run is None:
        return ReaperResult(
            pipeline=pipeline_name,
            expired=True,
            last_run=None,
            expiry_seconds=expiry_seconds,
            seconds_since=None,
        )
    elapsed = (now_fn() - last_run).total_seconds()
    return ReaperResult(
        pipeline=pipeline_name,
        expired=elapsed > expiry_seconds,
        last_run=last_run,
        expiry_seconds=expiry_seconds,
        seconds_since=elapsed,
    )


def check_all_reaper(
    app_cfg: AppConfig,
    store: StateStore,
    results: List[CheckResult],
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[ReaperResult]:
    out: List[ReaperResult] = []
    result_map = {r.pipeline: r for r in results}
    for pipeline in app_cfg.pipelines:
        expiry = getattr(pipeline, "expiry_seconds", None)
        if expiry is None:
            continue
        cr = result_map.get(pipeline.name)
        if cr and cr.status == CheckStatus.OK:
            continue
        out.append(check_reaper(pipeline.name, expiry, store, now_fn))
    return out
