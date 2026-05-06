"""Tests for pipewatch.census_cmd."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta

import pytest
from click.testing import CliRunner

from pipewatch.census_cmd import census_cmd
from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.checker import CheckStatus


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


def _make_app_cfg(history_file: Path, pipeline_names: list[str]) -> MagicMock:
    cfg = MagicMock()
    cfg.history_file = str(history_file)
    cfg.pipelines = [MagicMock(name=n) for n in pipeline_names]
    # MagicMock names need explicit assignment
    for mock, name in zip(cfg.pipelines, pipeline_names):
        mock.name = name
    return cfg


def _ctx_obj(history_file: Path, pipeline_names: list[str]) -> dict:
    return {"config": _make_app_cfg(history_file, pipeline_names)}


def test_no_pipelines_prints_message(runner: CliRunner, history_file: Path) -> None:
    result = runner.invoke(
        census_cmd, ["check"], obj=_ctx_obj(history_file, [])
    )
    assert result.exit_code == 0
    assert "No pipelines" in result.output


def test_check_shows_summary(runner: CliRunner, history_file: Path) -> None:
    store = HistoryStore(str(history_file))
    ts = datetime.now(timezone.utc) - timedelta(seconds=60)
    store.append(HistoryEntry(pipeline="pipe_a", status=CheckStatus.OK, checked_at=ts, last_run=None))

    result = runner.invoke(
        census_cmd, ["check", "--window", "3600"],
        obj=_ctx_obj(history_file, ["pipe_a"]),
    )
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_check_default_window(runner: CliRunner, history_file: Path) -> None:
    result = runner.invoke(
        census_cmd, ["check"],
        obj=_ctx_obj(history_file, ["pipe_x"]),
    )
    assert result.exit_code == 0
    assert "pipe_x" in result.output
