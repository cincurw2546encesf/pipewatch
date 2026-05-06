"""Tests for pipewatch.reaper_cmd."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.reaper_cmd import reaper_cmd
from pipewatch.state import StateStore, RunRecord
from pipewatch.config import AppConfig, PipelineConfig


DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def state_file(tmp_path: Path) -> str:
    return str(tmp_path / "state.json")


@pytest.fixture()
def ctx_obj(state_file: str):
    cfg = AppConfig(
        pipelines=[PipelineConfig(name="pipe", expiry_seconds=3600)],
        state_file=state_file,
    )
    return {"config": cfg}


def _write_state(state_file: str, finished: datetime) -> None:
    store = StateStore(state_file)
    store.record(RunRecord(
        pipeline="pipe",
        started="2024-06-01T11:00:00+00:00",
        finished=finished.isoformat(),
        status="ok",
    ))


def invoke(runner, ctx_obj, *args):
    return runner.invoke(reaper_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_check_no_record_exits_nonzero(runner, ctx_obj, state_file):
    result = invoke(runner, ctx_obj, "check", "pipe", "--expiry", "3600")
    assert result.exit_code == 1
    assert "never run" in result.output


def test_check_recent_run_exits_zero(runner, ctx_obj, state_file):
    _write_state(state_file, DT - timedelta(seconds=60))
    result = invoke(runner, ctx_obj, "check", "pipe", "--expiry", "3600")
    assert result.exit_code == 0
    assert "ok" in result.output


def test_check_expired_run_exits_nonzero(runner, ctx_obj, state_file):
    _write_state(state_file, DT - timedelta(seconds=7200))
    result = invoke(runner, ctx_obj, "check", "pipe", "--expiry", "3600")
    assert result.exit_code == 1
    assert "expired" in result.output


def test_list_no_expiry_pipelines(runner, state_file):
    cfg = AppConfig(
        pipelines=[PipelineConfig(name="pipe")],
        state_file=state_file,
    )
    result = runner.invoke(reaper_cmd, ["list"], obj={"config": cfg}, catch_exceptions=False)
    assert result.exit_code == 0
    assert "No pipelines" in result.output


def test_list_shows_expiry_pipelines(runner, ctx_obj, state_file):
    _write_state(state_file, DT - timedelta(seconds=60))
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "pipe" in result.output
