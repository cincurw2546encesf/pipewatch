"""Tests for pipewatch.budget_cmd."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.budget_cmd import budget_cmd
from pipewatch.checker import CheckResult, CheckStatus


def _utc(h: int = 0, m: int = 0, s: int = 0) -> datetime:
    return datetime(2024, 1, 1, h, m, s, tzinfo=timezone.utc)


def _make_pipeline(name: str, max_runtime: float | None = None):
    p = MagicMock()
    p.name = name
    p.max_runtime_seconds = max_runtime
    return p


def _make_result(
    name: str,
    last_run: datetime | None,
    last_finished: datetime | None,
) -> CheckResult:
    r = MagicMock(spec=CheckResult)
    r.pipeline = name
    r.status = CheckStatus.OK
    r.last_run = last_run
    r.last_finished = last_finished
    return r


@pytest.fixture()
def runner():
    return CliRunner()


def _make_app_cfg(pipelines):
    cfg = MagicMock()
    cfg.pipelines = pipelines
    return cfg


def test_check_no_budgets_configured(runner):
    app_cfg = _make_app_cfg([_make_pipeline("pipe_a", max_runtime=None)])
    ctx_obj = {"results": [], "app_cfg": app_cfg}
    result = runner.invoke(budget_cmd, ["check"], obj=ctx_obj)
    assert result.exit_code == 0
    assert "No budget" in result.output


def test_check_all_within_budget(runner):
    pl = _make_pipeline("pipe_a", max_runtime=120.0)
    app_cfg = _make_app_cfg([pl])
    res = _make_result("pipe_a", last_run=_utc(10, 0, 0), last_finished=_utc(10, 0, 30))
    ctx_obj = {"results": [res], "app_cfg": app_cfg}
    result = runner.invoke(budget_cmd, ["check"], obj=ctx_obj)
    assert result.exit_code == 0
    assert "within budget" in result.output


def test_check_exceeded_exits_nonzero(runner):
    pl = _make_pipeline("pipe_a", max_runtime=10.0)
    app_cfg = _make_app_cfg([pl])
    res = _make_result("pipe_a", last_run=_utc(10, 0, 0), last_finished=_utc(10, 5, 0))
    ctx_obj = {"results": [res], "app_cfg": app_cfg}
    result = runner.invoke(budget_cmd, ["check"], obj=ctx_obj)
    assert result.exit_code == 2
    assert "EXCEEDED" in result.output


def test_check_no_app_cfg_exits_nonzero(runner):
    ctx_obj = {"results": [], "app_cfg": None}
    result = runner.invoke(budget_cmd, ["check"], obj=ctx_obj)
    assert result.exit_code != 0
