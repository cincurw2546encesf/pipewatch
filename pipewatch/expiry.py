"""Expiry tracking: flag pipelines whose last successful run is older than a configured expiry window."""
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
class ExpiryResult:
    pipeline: str
    max_age_seconds: Optional[int]
    last_ok_seconds_ago: Optional[float]
    expired: bool

    def summary(self) -> str:
        if self.max_age_seconds is None:
            return f"{self.pipeline}: no expiry configured"
        if self.last_ok_seconds_ago is None:
            return f"{self.pipeline}: never run — considered expired"
        age_h = self.last_ok_seconds_ago / 3600
        limit_h = self.max_age_seconds / 3600
        status = "EXPIRED" if self.expired else "ok"
        return (
            f"{self.pipeline}: last ok {age_h:.1f}h ago "
            f"(limit {limit_h:.1f}h) [{status}]"
        )


def check_expiry(
    pipeline: PipelineConfig,
    result: CheckResult,
    *,
    now_fn: Callable[[], datetime] = _utcnow,
) -> Optional[ExpiryResult]:
    max_age = getattr(pipeline, "max_age_seconds", None)
    if max_age is None:
        return None

    now = now_fn()
    last_ok: Optional[float] = None

    if result.status == CheckStatus.OK and result.last_run is not None:
        delta = (now - result.last_run).total_seconds()
        last_ok = delta
    elif result.last_run is not None and result.status != CheckStatus.OK:
        # last_run exists but last status was not OK — treat as unknown age
        last_ok = None
    else:
        last_ok = None

    expired = last_ok is None or last_ok > max_age
    return ExpiryResult(
        pipeline=pipeline.name,
        max_age_seconds=max_age,
        last_ok_seconds_ago=last_ok,
        expired=expired,
    )


def check_all_expiry(
    pipelines: List[PipelineConfig],
    results: List[CheckResult],
    *,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[ExpiryResult]:
    result_map = {r.pipeline: r for r in results}
    out: List[ExpiryResult] = []
    for p in pipelines:
        r = result_map.get(p.name)
        if r is None:
            continue
        er = check_expiry(p, r, now_fn=now_fn)
        if er is not None:
            out.append(er)
    return out
