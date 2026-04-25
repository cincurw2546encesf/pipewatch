"""Tests for pipewatch.budget."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.budget import (
    BudgetConfig,
    BudgetResult,
    check_budget,
    check_all_budgets,
)
from pipewatch.checker import CheckResult, CheckStatus


def _utc(h: int = 0, m: int = 0, s: int = 0) -> datetime:
    return datetime(2024, 1, 1, h, m, s, tzinfo=timezone.utc)


def _result(
    name: str = "pipe",
    status: CheckStatus = CheckStatus.OK,
    last_run: datetime | None = None,
    last_finished: datetime | None = None,
) -> CheckResult:
    r = MagicMock(spec=CheckResult)
    r.pipeline = name
    r.status = status
    r.last_run = last_run
    r.last_finished = last_finished
    return r


def test_check_budget_no_runtime_returns_none_actual():
    r = _result(last_run=None, last_finished=None)
    br = check_budget(r, max_seconds=60)
    assert br.actual_seconds is None
    assert br.exceeded is False


def test_check_budget_within_limit():
    start = _utc(10, 0, 0)
    end = _utc(10, 0, 30)
    r = _result(last_run=start, last_finished=end)
    br = check_budget(r, max_seconds=60)
    assert br.actual_seconds == pytest.approx(30.0)
    assert br.exceeded is False


def test_check_budget_exactly_at_limit():
    start = _utc(10, 0, 0)
    end = _utc(10, 1, 0)
    r = _result(last_run=start, last_finished=end)
    br = check_budget(r, max_seconds=60)
    assert br.exceeded is False


def test_check_budget_exceeds_limit():
    start = _utc(10, 0, 0)
    end = _utc(10, 2, 0)
    r = _result(last_run=start, last_finished=end)
    br = check_budget(r, max_seconds=60)
    assert br.actual_seconds == pytest.approx(120.0)
    assert br.exceeded is True


def test_summary_exceeded():
    br = BudgetResult(pipeline="pipe", max_seconds=60, actual_seconds=120, exceeded=True)
    assert "EXCEEDED" in br.summary()
    assert "pipe" in br.summary()


def test_summary_ok():
    br = BudgetResult(pipeline="pipe", max_seconds=60, actual_seconds=30, exceeded=False)
    assert "OK" in br.summary()


def test_summary_no_runtime():
    br = BudgetResult(pipeline="pipe", max_seconds=60, actual_seconds=None, exceeded=False)
    assert "no runtime" in br.summary()


def test_check_all_budgets_filters_to_configured():
    r1 = _result("a", last_run=_utc(10), last_finished=_utc(10, 0, 30))
    r2 = _result("b", last_run=_utc(10), last_finished=_utc(10, 5, 0))
    r3 = _result("c", last_run=_utc(10), last_finished=_utc(10, 0, 10))
    budgets = [
        BudgetConfig(pipeline="a", max_seconds=60),
        BudgetConfig(pipeline="b", max_seconds=60),
    ]
    results = check_all_budgets([r1, r2, r3], budgets)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}


def test_check_all_budgets_empty_configs():
    r1 = _result("a", last_run=_utc(10), last_finished=_utc(10, 0, 30))
    results = check_all_budgets([r1], [])
    assert results == []
