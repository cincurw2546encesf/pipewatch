"""Tests for pipewatch.carryover_cmd."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.carryover_cmd import carryover_cmd
from pipewatch.config import AppConfig, PipelineConfig


DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def runner():
    return CliRunner()


def _make_pipeline(name, max_carryover_seconds=None):
    p = PipelineConfig(name=name, schedule="@hourly", max_staleness_seconds=3600)
    p.max_carryover_seconds = max_carryover_seconds
    return p


def _make_app_cfg(tmp_path, pipelines):
    state_file = tmp_path / "state.json"
    return AppConfig(pipelines=pipelines, state_file=str(state_file))


def _write_state(state_file, name, started_iso, finished_iso=None):
    data = {
        name: {
            "started": started_iso,
            "finished": finished_iso,
            "status": "running" if finished_iso is None else "ok",
        }
    }
    Path(state_file).write_text(json.dumps(data))


def _invoke(runner, tmp_path, pipelines, state_data=None):
    app_cfg = _make_app_cfg(tmp_path, pipelines)
    if state_data:
        Path(app_cfg.state_file).write_text(json.dumps(state_data))
    ctx_obj = {"app_cfg": app_cfg}
    return runner.invoke(carryover_cmd, ["check"], obj=ctx_obj)


def test_no_active_runs_exits_zero(runner, tmp_path):
    p = _make_pipeline("pipe", max_carryover_seconds=300)
    result = _invoke(runner, tmp_path, [p])
    assert result.exit_code == 0
    assert "No active" in result.output


def test_running_within_limit_exits_zero(runner, tmp_path):
    p = _make_pipeline("pipe", max_carryover_seconds=7200)
    state = {"pipe": {"started": "2024-06-01T11:55:00+00:00", "finished": None, "status": "running"}}
    result = _invoke(runner, tmp_path, [p], state_data=state)
    assert result.exit_code == 0
    assert "OK" in result.output


def test_exceeded_carryover_exits_nonzero(runner, tmp_path):
    p = _make_pipeline("pipe", max_carryover_seconds=60)
    state = {"pipe": {"started": "2024-06-01T11:00:00+00:00", "finished": None, "status": "running"}}
    result = _invoke(runner, tmp_path, [p], state_data=state)
    assert result.exit_code == 1
    assert "WARN" in result.output
