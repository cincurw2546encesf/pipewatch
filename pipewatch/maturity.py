"""Pipeline maturity scoring — rates pipelines based on health indicators."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.history import HistoryStore
from pipewatch.trend import analyse_trend


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MaturityScore:
    pipeline: str
    score: int          # 0–100
    grade: str          # A / B / C / D / F
    reasons: List[str]

    def summary(self) -> str:
        return f"{self.pipeline}: {self.grade} ({self.score}/100)"

    @property
    def healthy(self) -> bool:
        return self.score >= 70


def _grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 50:
        return "D"
    return "F"


def score_pipeline(
    result: CheckResult,
    store: Optional[HistoryStore] = None,
    now_fn=_utcnow,
) -> MaturityScore:
    score = 100
    reasons: List[str] = []

    if result.status == CheckStatus.FAILED:
        score -= 40
        reasons.append("last run failed (-40)")
    elif result.status == CheckStatus.STALE:
        score -= 20
        reasons.append("last run is stale (-20)")
    elif result.status == CheckStatus.MISSING:
        score -= 30
        reasons.append("no run recorded (-30)")

    if store is not None:
        entries = store.get(result.pipeline)
        trend = analyse_trend(entries)
        if trend.bad:
            score -= 20
            reasons.append(f"degrading trend ({trend.failure_rate:.0%} failure rate) (-20)")
        elif len(entries) < 5:
            score -= 10
            reasons.append("insufficient history (<5 runs) (-10)")

    score = max(0, min(100, score))
    return MaturityScore(
        pipeline=result.pipeline,
        score=score,
        grade=_grade(score),
        reasons=reasons,
    )


def score_all(
    results: List[CheckResult],
    store: Optional[HistoryStore] = None,
) -> List[MaturityScore]:
    return [score_pipeline(r, store=store) for r in results]
