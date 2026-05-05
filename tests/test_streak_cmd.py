"""Tests for pipewatch.streak_cmd."""
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.streak import StreakStore
from pipewatch.streak_cmd import streak_cmd


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def ctx_obj(tmp_path: Path) -> dict:
    class _Cfg:
        state_dir = str(tmp_path)

    return {"app_cfg": _Cfg()}


def invoke(runner: CliRunner, ctx_obj: dict, *args: str):
    return runner.invoke(streak_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_list_no_streaks(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No streak data" in result.output


def test_list_shows_entry(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = StreakStore(tmp_path / "streaks.json")
    store.record("my_pipeline", "ok")
    store.record("my_pipeline", "ok")

    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "my_pipeline" in result.output
    assert "x2" in result.output


def test_reset_existing_entry(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = StreakStore(tmp_path / "streaks.json")
    store.record("my_pipeline", "fail")

    result = invoke(runner, ctx_obj, "reset", "my_pipeline")
    assert result.exit_code == 0
    assert "reset" in result.output.lower()

    reloaded = StreakStore(tmp_path / "streaks.json")
    assert reloaded.get("my_pipeline") is None


def test_reset_unknown_pipeline(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "reset", "ghost_pipeline")
    assert result.exit_code == 0
    assert "No streak entry" in result.output
