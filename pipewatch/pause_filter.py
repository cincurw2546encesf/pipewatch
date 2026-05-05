"""Filter check results based on pause state."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from pipewatch.checker import CheckResult
from pipewatch.pause import PauseStore


@dataclass
class PauseFilterSummary:
    active: List[CheckResult]
    skipped: List[CheckResult]

    def __str__(self) -> str:
        return (
            f"PauseFilter: {len(self.active)} active, "
            f"{len(self.skipped)} skipped (paused)"
        )


def filter_paused(
    results: List[CheckResult],
    store: PauseStore,
) -> Tuple[List[CheckResult], List[CheckResult]]:
    """Split results into (active, skipped) based on pause store."""
    active: List[CheckResult] = []
    skipped: List[CheckResult] = []
    for result in results:
        if store.is_paused(result.pipeline):
            skipped.append(result)
        else:
            active.append(result)
    return active, skipped


def pause_summary(
    results: List[CheckResult],
    store: PauseStore,
) -> PauseFilterSummary:
    """Return a PauseFilterSummary for the given results."""
    active, skipped = filter_paused(results, store)
    return PauseFilterSummary(active=active, skipped=skipped)
