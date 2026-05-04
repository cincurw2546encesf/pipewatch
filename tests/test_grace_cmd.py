"""Tests for pipewatch.grace_cmd."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.grace_cmd import grace_cmd


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def ctx_obj(tmp_path: Path) -> dict:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    return {"app_cfg": cfg}


def invoke(runner: CliRunner, ctx_obj: dict, *args: str):
    return runner.invoke(grace_cmd, list(args), obj=ctx_obj, catch_exceptions=False)


def test_list_no_graces(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No grace periods" in result.output


def test_add_grace_output(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "add", "my_pipe", "300")
    assert result.exit_code == 0
    assert "my_pipe" in result.output
    assert "300" in result.output


def test_list_shows_registered(runner: CliRunner, ctx_obj: dict) -> None:
    invoke(runner, ctx_obj, "add", "pipe_x", "120")
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "pipe_x" in result.output


def test_remove_grace(runner: CliRunner, ctx_obj: dict) -> None:
    invoke(runner, ctx_obj, "add", "pipe_y", "60")
    result = invoke(runner, ctx_obj, "remove", "pipe_y")
    assert result.exit_code == 0
    assert "removed" in result.output


def test_remove_missing_grace(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "remove", "ghost_pipe")
    assert result.exit_code == 0
    assert "No grace period found" in result.output
