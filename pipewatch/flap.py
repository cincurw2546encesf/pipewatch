"""Flap detection: identify pipelines that oscillate between OK and non-OK states."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.history import HistoryEntry, HistoryStore


DEFAULT_WINDOW = 10  # number of recent history entries to inspect
DEFAULT_THRESHOLD = 3  # minimum status transitions to be considered flapping


@dataclass
class FlapResult:
    pipeline: str
    transitions: int
    window: int
    is_flapping: bool

    @property
    def summary(self) -> str:
        state = "FLAPPING" if self.is_flapping else "stable"
        return (
            f"{self.pipeline}: {state} "
            f"({self.transitions} transitions in last {self.window} runs)"
        )


def _count_transitions(entries: List[HistoryEntry]) -> int:
    """Count the number of times the status changes between consecutive entries."""
    if len(entries) < 2:
        return 0
    transitions = 0
    for i in range(1, len(entries)):
        if entries[i].status != entries[i - 1].status:
            transitions += 1
    return transitions


def check_flap(
    pipeline: str,
    store: HistoryStore,
    window: int = DEFAULT_WINDOW,
    threshold: int = DEFAULT_THRESHOLD,
) -> FlapResult:
    """Analyse recent history for a pipeline and return a FlapResult."""
    entries = store.get(pipeline)
    recent = entries[-window:] if len(entries) > window else entries
    transitions = _count_transitions(recent)
    return FlapResult(
        pipeline=pipeline,
        transitions=transitions,
        window=len(recent),
        is_flapping=transitions >= threshold,
    )


def check_all_flap(
    pipelines: List[str],
    store: HistoryStore,
    window: int = DEFAULT_WINDOW,
    threshold: int = DEFAULT_THRESHOLD,
) -> List[FlapResult]:
    """Return flap results for every pipeline in the list."""
    return [check_flap(p, store, window, threshold) for p in pipelines]
