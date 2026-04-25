"""Tests for pipewatch.ratelimit_cmd CLI commands."""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.ratelimit import RateLimitStore
from pipewatch.ratelimit_cmd import ratelimit_cmd


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def ctx_obj(tmp_path: Path) -> dict:
    class _Cfg:
        state_dir = str(tmp_path)

    return {"app_cfg": _Cfg()}


def invoke(runner: CliRunner, ctx_obj: dict, *args: str):
    return runner.invoke(ratelimit_cmd, list(args), obj=ctx_obj)


def test_list_no_ratelimits(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No rate limit records" in result.output


def test_list_shows_entry(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = RateLimitStore(tmp_path / "ratelimit.json")
    store.record_check("my_pipeline")
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "my_pipeline" in result.output
    assert "checks=1" in result.output


def test_reset_clears_entry(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = RateLimitStore(tmp_path / "ratelimit.json")
    store.record_check("pipe_x")
    result = invoke(runner, ctx_obj, "reset", "pipe_x")
    assert result.exit_code == 0
    assert "cleared" in result.output
    reloaded = RateLimitStore(tmp_path / "ratelimit.json")
    assert reloaded.get("pipe_x") is None


def test_reset_nonexistent_pipeline(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "reset", "ghost")
    assert result.exit_code == 0
    assert "cleared" in result.output
