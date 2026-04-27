"""Tests for pipewatch.carryover."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.carryover import CarryoverResult, check_carryover, check_all_carryover
from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.state import StateStore


DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def state_file(tmp_path):
    return tmp_path / "state.json"


@pytest.fixture
def store(state_file):
    return StateStore(state_file)


def _pipeline(name="pipe", max_carryover_seconds=None):
    p = PipelineConfig(name=name, schedule="@hourly", max_staleness_seconds=3600)
    p.max_carryover_seconds = max_carryover_seconds
    return p


def _write_started(state_file, name, started_iso, finished_iso=None):
    data = {
        name: {
            "started": started_iso,
            "finished": finished_iso,
            "status": "running" if finished_iso is None else "ok",
        }
    }
    state_file.write_text(json.dumps(data))


def test_no_record_returns_none(store):
    p = _pipeline(max_carryover_seconds=300)
    result = check_carryover(p, store, now_fn=lambda: DT)
    assert result is None


def test_finished_run_returns_none(state_file):
    _write_started(state_file, "pipe", "2024-06-01T11:00:00+00:00", "2024-06-01T11:30:00+00:00")
    store = StateStore(state_file)
    p = _pipeline(max_carryover_seconds=300)
    result = check_carryover(p, store, now_fn=lambda: DT)
    assert result is None


def test_running_within_limit(state_file):
    _write_started(state_file, "pipe", "2024-06-01T11:55:00+00:00")
    store = StateStore(state_file)
    p = _pipeline(max_carryover_seconds=600)
    result = check_carryover(p, store, now_fn=lambda: DT)
    assert result is not None
    assert result.exceeded is False
    assert result.carried_over_seconds == pytest.approx(300.0)


def test_running_exceeds_limit(state_file):
    _write_started(state_file, "pipe", "2024-06-01T11:00:00+00:00")
    store = StateStore(state_file)
    p = _pipeline(max_carryover_seconds=300)
    result = check_carryover(p, store, now_fn=lambda: DT)
    assert result is not None
    assert result.exceeded is True
    assert result.carried_over_seconds == pytest.approx(3600.0)


def test_no_limit_never_exceeded(state_file):
    _write_started(state_file, "pipe", "2024-06-01T11:00:00+00:00")
    store = StateStore(state_file)
    p = _pipeline(max_carryover_seconds=None)
    result = check_carryover(p, store, now_fn=lambda: DT)
    assert result is not None
    assert result.exceeded is False
    assert result.max_carryover_seconds is None


def test_summary_no_started():
    r = CarryoverResult(pipeline="p", started_at=None, carried_over_seconds=None, exceeded=False, max_carryover_seconds=None)
    assert "no active run" in r.summary()


def test_summary_exceeded():
    r = CarryoverResult(
        pipeline="pipe", started_at=DT, carried_over_seconds=3700.0,
        exceeded=True, max_carryover_seconds=300,
    )
    assert "STUCK" in r.summary()
    assert "3700" in r.summary()


def test_check_all_carryover_filters_completed(state_file):
    _write_started(state_file, "pipe", "2024-06-01T11:00:00+00:00", "2024-06-01T11:30:00+00:00")
    store = StateStore(state_file)
    p = _pipeline("pipe", max_carryover_seconds=300)
    app_cfg = AppConfig(pipelines=[p], state_file=str(state_file))
    results = check_all_carryover(app_cfg, store, now_fn=lambda: DT)
    assert results == []
