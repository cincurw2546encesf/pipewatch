"""Daily/periodic digest report summarising pipeline health across all pipelines."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.trend import TrendResult


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DigestEntry:
    pipeline: str
    status: CheckStatus
    last_run: str | None
    failure_rate: float
    trend: str


@dataclass
class DigestReport:
    generated_at: str
    total: int
    healthy: int
    stale: int
    failed: int
    entries: List[DigestEntry] = field(default_factory=list)

    @property
    def summary_line(self) -> str:
        return (
            f"Digest [{self.generated_at}] — "
            f"{self.total} pipelines: "
            f"{self.healthy} OK, {self.stale} stale, {self.failed} failed"
        )


def build_digest(
    results: List[CheckResult],
    trends: dict[str, TrendResult] | None = None,
    now_fn=_utcnow,
) -> DigestReport:
    trends = trends or {}
    healthy = sum(1 for r in results if r.status == CheckStatus.OK)
    stale = sum(1 for r in results if r.status == CheckStatus.STALE)
    failed = sum(1 for r in results if r.status == CheckStatus.FAILED)
    entries = []
    for r in results:
        tr = trends.get(r.pipeline)
        entries.append(
            DigestEntry(
                pipeline=r.pipeline,
                status=r.status,
                last_run=r.last_run.isoformat() if r.last_run else None,
                failure_rate=round(tr.failure_rate, 3) if tr else 0.0,
                trend=tr.direction if tr else "stable",
            )
        )
    return DigestReport(
        generated_at=now_fn().strftime("%Y-%m-%dT%H:%M:%SZ"),
        total=len(results),
        healthy=healthy,
        stale=stale,
        failed=failed,
        entries=entries,
    )


def format_digest_text(report: DigestReport) -> str:
    lines = [report.summary_line, ""]
    for e in report.entries:
        icon = {CheckStatus.OK: "✓", CheckStatus.STALE: "~", CheckStatus.FAILED: "✗"}.get(e.status, "?")
        lines.append(
            f"  {icon} {e.pipeline:<30} last={e.last_run or 'never':<25} "
            f"fail_rate={e.failure_rate:.1%}  trend={e.trend}"
        )
    return "\n".join(lines)
