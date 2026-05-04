"""Utility to filter CheckResults that are within an active grace period."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.grace import GraceStore


@dataclass
class GraceFilterSummary:
    total: int
    suppressed: int
    passed: int

    def __str__(self) -> str:
        return (
            f"GraceFilter: {self.total} results, "
            f"{self.suppressed} suppressed (grace), "
            f"{self.passed} passed through"
        )


def filter_grace(
    results: Sequence[CheckResult],
    store: GraceStore,
    now: Optional[datetime] = None,
) -> list[CheckResult]:
    """Return only results that are NOT within an active grace period.

    Results with status OK are always passed through unchanged.
    Non-OK results for pipelines in grace are dropped.
    """
    passed: list[CheckResult] = []
    for r in results:
        if r.status == CheckStatus.OK:
            passed.append(r)
        elif store.is_in_grace(r.pipeline, now=now):
            # Suppress non-OK alert during grace window
            continue
        else:
            passed.append(r)
    return passed


def grace_summary(
    results: Sequence[CheckResult],
    store: GraceStore,
    now: Optional[datetime] = None,
) -> GraceFilterSummary:
    """Return counts of how many results were suppressed by grace filtering."""
    passed = filter_grace(results, store, now=now)
    suppressed = len(results) - len(passed)
    return GraceFilterSummary(
        total=len(results),
        suppressed=suppressed,
        passed=len(passed),
    )
