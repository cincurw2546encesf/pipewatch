"""Tests for pipewatch.throttle_cmd CLI commands."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipewatch.throttle import ThrottleStore
from pipewatch.throttle_cmd import throttle_cmd


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def ctx_obj(tmp_path: Path) -> dict[str, Any]:
    class _Cfg:
        state_dir = str(tmp_path)

    return {"app_cfg": _Cfg()}


def invoke(runner: CliRunner, ctx_obj: dict, *args: str):
    return runner.invoke(throttle_cmd, list(args), obj=ctx_obj)


def test_list_no_throttles(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No throttled" in result.output


def test_list_shows_throttled_pipeline(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = ThrottleStore(tmp_path / "throttle.json")
    store.record_alert("my_pipeline")

    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "my_pipeline" in result.output


def test_reset_clears_entry(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = ThrottleStore(tmp_path / "throttle.json")
    store.record_alert("my_pipeline")

    result = invoke(runner, ctx_obj, "reset", "my_pipeline")
    assert result.exit_code == 0
    assert "cleared" in result.output

    reloaded = ThrottleStore(tmp_path / "throttle.json")
    assert reloaded.get("my_pipeline") is None


def test_reset_missing_pipeline(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "reset", "nonexistent")
    assert result.exit_code == 0
    assert "No throttle entry" in result.output
