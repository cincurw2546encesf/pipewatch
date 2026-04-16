"""Tests for baseline CLI commands."""
from __future__ import annotations
import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from pipewatch.baseline_cmd import baseline_cmd
from pipewatch.baseline import BaselineStore


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def ctx_obj(tmp_path: Path) -> dict:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    return {"app_cfg": cfg}


def invoke(runner, ctx_obj, *args):
    return runner.invoke(baseline_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_list_no_baselines(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No baselines" in result.output


def test_list_shows_entry(runner, ctx_obj, tmp_path):
    store = BaselineStore(tmp_path / "baselines.json")
    store.update("my_pipe", 45.0)
    result = invoke(runner, ctx_obj, "list")
    assert "my_pipe" in result.output
    assert "45.0" in result.output


def test_reset_existing(runner, ctx_obj, tmp_path):
    store = BaselineStore(tmp_path / "baselines.json")
    store.update("my_pipe", 10.0)
    result = invoke(runner, ctx_obj, "reset", "my_pipe")
    assert result.exit_code == 0
    assert "removed" in result.output


def test_reset_missing(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "reset", "ghost")
    assert result.exit_code == 0
    assert "No baseline" in result.output


def test_check_no_baseline(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "check", "pipe_x")
    assert result.exit_code == 0
    assert "no baseline" in result.output
