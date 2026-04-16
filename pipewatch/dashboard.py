"""Simple terminal dashboard summary for pipeline health."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List
from pipewatch.checker import CheckResult, CheckStatus


_ICONS = {
    CheckStatus.OK: "✅",
    CheckStatus.STALE: "⚠️",
    CheckStatus.FAILED: "❌",
    CheckStatus.MISSING: "❓",
}


@dataclass
class DashboardSummary:
    total: int
    ok: int
    stale: int
    failed: int
    missing: int

    @property
    def healthy(self) -> bool:
        return self.stale == 0 and self.failed == 0 and self.missing == 0


def build_dashboard(results: List[CheckResult]) -> str:
    lines: List[str] = []
    lines.append("=" * 48)
    lines.append(" PipeWatch Dashboard")
    lines.append("=" * 48)
    for r in results:
        icon = _ICONS.get(r.status, "?")
        last = r.last_run.strftime("%Y-%m-%d %H:%M UTC") if r.last_run else "never"
        lines.append(f"  {icon}  {r.pipeline:<20}  last: {last}")
        if r.message:
            lines.append(f"       {r.message}")
    lines.append("-" * 48)
    summary = summarise(results)
    lines.append(
        f"  Total: {summary.total}  OK: {summary.ok}  "
        f"Stale: {summary.stale}  Failed: {summary.failed}  Missing: {summary.missing}"
    )
    status_line = "Overall: HEALTHY ✅" if summary.healthy else "Overall: DEGRADED ⚠️"
    lines.append(f"  {status_line}")
    lines.append("=" * 48)
    return "\n".join(lines)


def summarise(results: List[CheckResult]) -> DashboardSummary:
    counts = {s: 0 for s in CheckStatus}
    for r in results:
        counts[r.status] += 1
    return DashboardSummary(
        total=len(results),
        ok=counts[CheckStatus.OK],
        stale=counts[CheckStatus.STALE],
        failed=counts[CheckStatus.FAILED],
        missing=counts[CheckStatus.MISSING],
    )
