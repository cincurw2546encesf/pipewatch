"""Triage module: rank pipelines by urgency for operator attention."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.checker import CheckResult, CheckStatus


# Priority weights — higher means more urgent
_WEIGHTS = {
    CheckStatus.FAILED: 100,
    CheckStatus.STALE: 50,
    CheckStatus.OK: 0,
}


@dataclass
class TriageEntry:
    result: CheckResult
    score: int

    @property
    def pipeline(self) -> str:
        return self.result.pipeline

    @property
    def status(self) -> CheckStatus:
        return self.result.status

    def summary(self) -> str:
        icon = "🔴" if self.status == CheckStatus.FAILED else "🟡" if self.status == CheckStatus.STALE else "🟢"
        return f"{icon} [{self.score:>3}] {self.pipeline}: {self.result.message}"


@dataclass
class TriageReport:
    entries: List[TriageEntry] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return all(e.status == CheckStatus.OK for e in self.entries)

    @property
    def top(self) -> TriageEntry | None:
        return self.entries[0] if self.entries else None

    def critical(self) -> List[TriageEntry]:
        return [e for e in self.entries if e.status == CheckStatus.FAILED]

    def actionable(self) -> List[TriageEntry]:
        return [e for e in self.entries if e.status != CheckStatus.OK]

    def summary(self) -> str:
        total = len(self.entries)
        n_crit = len(self.critical())
        n_stale = sum(1 for e in self.entries if e.status == CheckStatus.STALE)
        return (
            f"Triage: {total} pipeline(s) — "
            f"{n_crit} failed, {n_stale} stale, "
            f"{total - n_crit - n_stale} ok"
        )


def _score(result: CheckResult) -> int:
    """Compute a numeric urgency score for a single result."""
    base = _WEIGHTS.get(result.status, 0)
    # Boost score if there is no last-run timestamp (never ran)
    if result.last_run is None and result.status != CheckStatus.OK:
        base += 20
    return base


def triage(results: List[CheckResult]) -> TriageReport:
    """Rank results by urgency, most urgent first."""
    entries = [
        TriageEntry(result=r, score=_score(r))
        for r in results
    ]
    entries.sort(key=lambda e: e.score, reverse=True)
    return TriageReport(entries=entries)
