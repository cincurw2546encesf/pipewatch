"""Burst detection: flag pipelines that have run too many times in a short window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BurstResult:
    pipeline: str
    run_count: int
    window_seconds: int
    max_runs: int
    exceeded: bool
    oldest_in_window: Optional[datetime]
    newest_in_window: Optional[datetime]

    def summary(self) -> str:
        if self.exceeded:
            return (
                f"{self.pipeline}: BURST detected — "
                f"{self.run_count} runs in {self.window_seconds}s "
                f"(limit {self.max_runs})"
            )
        if self.run_count == 0:
            return f"{self.pipeline}: no runs in window"
        return (
            f"{self.pipeline}: {self.run_count} runs in "
            f"{self.window_seconds}s (limit {self.max_runs}) — OK"
        )


def check_burst(
    pipeline: PipelineConfig,
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> Optional[BurstResult]:
    """Return a BurstResult if the pipeline has burst config, else None."""
    max_runs: Optional[int] = getattr(pipeline, "burst_max_runs", None)
    window_seconds: Optional[int] = getattr(pipeline, "burst_window_seconds", None)

    if max_runs is None or window_seconds is None:
        return None

    now = now_fn()
    entries = store.get(pipeline.name)
    cutoff = now.timestamp() - window_seconds

    in_window = [
        e for e in entries
        if e.finished_at is not None and e.finished_at.timestamp() >= cutoff
    ]

    run_count = len(in_window)
    oldest = min((e.finished_at for e in in_window), default=None)
    newest = max((e.finished_at for e in in_window), default=None)

    return BurstResult(
        pipeline=pipeline.name,
        run_count=run_count,
        window_seconds=window_seconds,
        max_runs=max_runs,
        exceeded=run_count > max_runs,
        oldest_in_window=oldest,
        newest_in_window=newest,
    )


def check_all_burst(
    pipelines: List[PipelineConfig],
    store: HistoryStore,
    now_fn: Callable[[], datetime] = _utcnow,
) -> List[BurstResult]:
    results = []
    for p in pipelines:
        r = check_burst(p, store, now_fn=now_fn)
        if r is not None:
            results.append(r)
    return results
