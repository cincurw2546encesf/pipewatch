"""Tests for pipewatch.drain_cmd."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.drain_cmd import drain_cmd
from pipewatch.history import HistoryStore, HistoryEntry
from datetime import datetime, timezone


def _dt() -> datetime:
    return datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)


class _Pipeline:
    def __init__(self, name: str, min_duration_seconds=None):
        self.name = name
        self.min_duration_seconds = min_duration_seconds


class _AppCfg:
    def __init__(self, pipelines, history_file):
        self.pipelines = pipelines
        self.history_file = history_file


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


def _add(store: HistoryStore, pipeline: str, duration: float):
    entry = HistoryEntry(
        pipeline=pipeline,
        status="ok",
        checked_at=_dt().isoformat(),
        last_run=_dt().isoformat(),
        duration_seconds=duration,
    )
    store.append(pipeline, entry)


def _invoke(runner, history_file, pipelines, args=None):
    app_cfg = _AppCfg(pipelines, history_file)
    return runner.invoke(
        drain_cmd,
        args or ["check"],
        obj={"app_cfg": app_cfg},
        catch_exceptions=False,
    )


def test_no_configured_pipelines_exits_zero(runner, history_file):
    result = _invoke(runner, history_file, [_Pipeline("pipe_a")])
    assert result.exit_code == 0
    assert "No pipelines" in result.output


def test_all_ok_exits_zero(runner, history_file):
    store = HistoryStore(history_file)
    for _ in range(3):
        _add(store, "pipe_a", 15.0)
    pipelines = [_Pipeline("pipe_a", min_duration_seconds=5.0)]
    result = _invoke(runner, history_file, pipelines)
    assert result.exit_code == 0
    assert "\u2705" in result.output


def test_draining_exits_nonzero(runner, history_file):
    store = HistoryStore(history_file)
    for _ in range(3):
        _add(store, "pipe_a", 0.5)
    pipelines = [_Pipeline("pipe_a", min_duration_seconds=5.0)]
    result = _invoke(runner, history_file, pipelines)
    assert result.exit_code == 1
    assert "\u26a0" in result.output


def test_window_option_respected(runner, history_file):
    store = HistoryStore(history_file)
    # many old long runs, few recent short runs
    for _ in range(10):
        _add(store, "pipe_a", 20.0)
    for _ in range(2):
        _add(store, "pipe_a", 0.1)
    pipelines = [_Pipeline("pipe_a", min_duration_seconds=5.0)]
    result = _invoke(runner, history_file, pipelines, args=["check", "--window", "2"])
    assert result.exit_code == 1
