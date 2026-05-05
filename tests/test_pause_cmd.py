"""Tests for pipewatch.pause_cmd."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.pause_cmd import pause_cmd


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def ctx_obj(tmp_path: Path) -> dict:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    return {"app_cfg": cfg}


def invoke(runner: CliRunner, ctx_obj: dict, *args: str):
    return runner.invoke(pause_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_list_no_pauses(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No pipelines" in result.output


def test_add_pause_output(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "add", "etl_daily")
    assert result.exit_code == 0
    assert "Paused 'etl_daily'" in result.output


def test_add_pause_with_reason(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "add", "etl_daily", "--reason", "maintenance")
    assert result.exit_code == 0
    assert "maintenance" in result.output


def test_remove_pause(runner: CliRunner, ctx_obj: dict) -> None:
    invoke(runner, ctx_obj, "add", "etl_daily")
    result = invoke(runner, ctx_obj, "remove", "etl_daily")
    assert result.exit_code == 0
    assert "Resumed" in result.output


def test_remove_unknown_pipeline(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "remove", "unknown_pipe")
    assert result.exit_code == 0
    assert "not paused" in result.output


def test_list_shows_paused_pipeline(runner: CliRunner, ctx_obj: dict) -> None:
    invoke(runner, ctx_obj, "add", "etl_daily", "--reason", "deploy freeze")
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "etl_daily" in result.output
    assert "deploy freeze" in result.output
