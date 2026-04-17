"""Trend analysis: detect improving/degrading pipeline health over history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.checker import CheckStatus


@dataclass
class TrendResult:
    pipeline: str
    total: int
    ok_count: int
    stale_count: int
    failed_count: int
    trend: str  # "improving", "degrading", "stable", "insufficient_data"

    @property
    def failure_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.stale_count + self.failed_count) / self.total

    @property
    def ok_rate(self) -> float:
        """Fraction of runs that completed successfully."""
        if self.total == 0:
            return 0.0
        return self.ok_count / self.total


def _trend_direction(entries: List[HistoryEntry], window: int = 5) -> str:
    if len(entries) < window * 2:
        return "insufficient_data"
    older = entries[:window]
    newer = entries[-window:]

    def bad(e: HistoryEntry) -> int:
        return 1 if e.status in (CheckStatus.STALE.value, CheckStatus.FAILED.value) else 0

    older_bad = sum(bad(e) for e in older)
    newer_bad = sum(bad(e) for e in newer)

    if newer_bad < older_bad:
        return "improving"
    elif newer_bad > older_bad:
        return "degrading"
    return "stable"


def analyse_trend(pipeline: str, store: HistoryStore, limit: int = 50) -> TrendResult:
    entries = store.get(pipeline, limit=limit)
    total = len(entries)
    ok_count = sum(1 for e in entries if e.status == CheckStatus.OK.value)
    stale_count = sum(1 for e in entries if e.status == CheckStatus.STALE.value)
    failed_count = sum(1 for e in entries if e.status == CheckStatus.FAILED.value)
    trend = _trend_direction(entries)
    return TrendResult(
        pipeline=pipeline,
        total=total,
        ok_count=ok_count,
        stale_count=stale_count,
        failed_count=failed_count,
        trend=trend,
    )


def analyse_all(pipelines: List[str], store: HistoryStore, limit: int = 50) -> List[TrendResult]:
    return [analyse_trend(p, store, limit=limit) for p in pipelines]
