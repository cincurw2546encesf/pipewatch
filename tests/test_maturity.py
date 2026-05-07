"""Tests for pipewatch.maturity."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.history import HistoryStore
from pipewatch.maturity import MaturityScore, _grade, score_all, score_pipeline


def _utc(year=2024, month=1, day=1, hour=0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def _result(name: str, status: CheckStatus) -> CheckResult:
    return CheckResult(
        pipeline=name,
        status=status,
        message="",
        last_run=_utc() if status != CheckStatus.MISSING else None,
    )


# ---------------------------------------------------------------------------
# _grade helper
# ---------------------------------------------------------------------------

def test_grade_a():
    assert _grade(95) == "A"


def test_grade_b():
    assert _grade(85) == "B"


def test_grade_c():
    assert _grade(70) == "C"


def test_grade_d():
    assert _grade(55) == "D"


def test_grade_f():
    assert _grade(40) == "F"


# ---------------------------------------------------------------------------
# score_pipeline — no history store
# ---------------------------------------------------------------------------

def test_ok_pipeline_scores_100():
    r = _result("pipe_ok", CheckStatus.OK)
    s = score_pipeline(r)
    assert s.score == 100
    assert s.grade == "A"
    assert s.healthy is True
    assert s.reasons == []


def test_stale_pipeline_loses_20():
    r = _result("pipe_stale", CheckStatus.STALE)
    s = score_pipeline(r)
    assert s.score == 80
    assert s.grade == "B"


def test_failed_pipeline_loses_40():
    r = _result("pipe_fail", CheckStatus.FAILED)
    s = score_pipeline(r)
    assert s.score == 60
    assert s.grade == "D"
    assert any("failed" in reason for reason in s.reasons)


def test_missing_pipeline_loses_30():
    r = _result("pipe_miss", CheckStatus.MISSING)
    s = score_pipeline(r)
    assert s.score == 70
    assert s.grade == "C"


# ---------------------------------------------------------------------------
# score_pipeline — with history store
# ---------------------------------------------------------------------------

def test_thin_history_penalised(tmp_path: Path):
    store = HistoryStore(tmp_path / "history.json")
    r = _result("pipe_new", CheckStatus.OK)
    s = score_pipeline(r, store=store)
    # fewer than 5 entries → -10
    assert s.score == 90
    assert any("insufficient" in reason for reason in s.reasons)


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------

def test_score_all_returns_one_per_result():
    results = [
        _result("a", CheckStatus.OK),
        _result("b", CheckStatus.STALE),
        _result("c", CheckStatus.FAILED),
    ]
    scores = score_all(results)
    assert len(scores) == 3
    names = {s.pipeline for s in scores}
    assert names == {"a", "b", "c"}


def test_score_all_empty():
    assert score_all([]) == []


# ---------------------------------------------------------------------------
# MaturityScore.summary
# ---------------------------------------------------------------------------

def test_summary_format():
    s = MaturityScore(pipeline="my_pipe", score=85, grade="B", reasons=[])
    assert s.summary() == "my_pipe: B (85/100)"
