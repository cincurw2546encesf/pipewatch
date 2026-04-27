"""Tests for the capacity CLI command."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.capacity_cmd import capacity_cmd
from pipewatch.capacity import CapacityResult
from pipewatch.config import AppConfig, PipelineConfig


@pytest.fixture()
def runner():
    return CliRunner()


def _make_pipeline(name: str, budget: float = 120.0) -> PipelineConfig:
    p = PipelineConfig(name=name, max_age_minutes=60)
    object.__setattr__(p, "budget_seconds", budget)
    return p


def _make_app_cfg(*names) -> AppConfig:
    return AppConfig(pipelines=[_make_pipeline(n) for n in names])


def _ctx_obj(app_cfg: AppConfig, history_file: str = "h.json") -> dict:
    return {"config": app_cfg, "history_file": history_file}


def _invoke(runner, app_cfg, results: List[CapacityResult], extra_args=None):
    args = ["check"] + (extra_args or [])
    with patch("pipewatch.capacity_cmd.check_all_capacity", return_value=results):
        with patch("pipewatch.capacity_cmd.HistoryStore"):
            return runner.invoke(
                capacity_cmd,
                args,
                obj=_ctx_obj(app_cfg),
                catch_exceptions=False,
            )


def test_all_ok_exits_zero(runner):
    cfg = _make_app_cfg("pipe_a")
    results = [
        CapacityResult("pipe_a", 30.0, 120.0, 0.25, False, 5),
    ]
    result = _invoke(runner, cfg, results)
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_exceeded_exits_nonzero(runner):
    cfg = _make_app_cfg("pipe_b")
    results = [
        CapacityResult("pipe_b", 200.0, 120.0, 1.67, True, 5),
    ]
    result = _invoke(runner, cfg, results)
    assert result.exit_code == 1


def test_output_contains_summary(runner):
    cfg = _make_app_cfg("pipe_c")
    results = [
        CapacityResult("pipe_c", 60.0, 120.0, 0.5, False, 3),
    ]
    result = _invoke(runner, cfg, results)
    assert "pipe_c" in result.output


def test_custom_window_passed_through(runner):
    cfg = _make_app_cfg("pipe_d")
    results = [CapacityResult("pipe_d", None, 120.0, None, False, 0)]
    with patch("pipewatch.capacity_cmd.check_all_capacity", return_value=results) as mock_fn:
        with patch("pipewatch.capacity_cmd.HistoryStore"):
            runner.invoke(
                capacity_cmd,
                ["check", "--window", "20"],
                obj=_ctx_obj(cfg),
                catch_exceptions=False,
            )
    mock_fn.assert_called_once()
    _, kwargs = mock_fn.call_args
    assert kwargs.get("window") == 20 or mock_fn.call_args[0][2] == 20
