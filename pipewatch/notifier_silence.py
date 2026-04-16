"""Integration: suppress notifications for silenced pipelines."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pipewatch.checker import CheckResult
from pipewatch.silencer import SilenceStore


def filter_silenced(
    results: list[CheckResult],
    store: SilenceStore,
    now: Optional[datetime] = None,
) -> tuple[list[CheckResult], list[CheckResult]]:
    """Split results into (active, silenced) based on the silence store.

    Returns:
        active:   results that should proceed to notification.
        silenced: results that were suppressed.
    """
    active: list[CheckResult] = []
    silenced: list[CheckResult] = []
    for r in results:
        if store.is_silenced(r.pipeline, now):
            silenced.append(r)
        else:
            active.append(r)
    return active, silenced


def silenced_summary(silenced: list[CheckResult]) -> str:
    """Return a human-readable summary of suppressed pipelines."""
    if not silenced:
        return "No alerts suppressed by silences."
    names = ", ".join(r.pipeline for r in silenced)
    return f"Suppressed alerts for {len(silenced)} silenced pipeline(s): {names}"
