"""Tests for pipewatch.velocity."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.velocity import VelocityResult, check_velocity, check_all_velocity


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _now_fn():
    return NOW


def _pipeline(name="pipe", window_hours=24, min_runs=3) -> PipelineConfig:
    p = PipelineConfig(name=name, schedule_minutes=60, max_age_minutes=120)
    p.velocity_window_hours = window_hours
    p.velocity_min_runs = min_runs
    return p


def _entry(minutes_ago: int, status: str = "ok") -> HistoryEntry:
    ts = NOW - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        pipeline="pipe",
        status=status,
        checked_at=ts,
        last_run=None,
        message="",
    )


@pytest.fixture()
def store(tmp_path) -> HistoryStore:
    return HistoryStore(str(tmp_path / "history.json"))


def test_no_velocity_config_returns_none(store):
    p = PipelineConfig(name="pipe", schedule_minutes=60, max_age_minutes=120)
    assert check_velocity(p, store, now_fn=_now_fn) is None


def test_sufficient_runs_not_exceeded(store):
    for m in [30, 60, 90]:
        store.append(_entry(m))
    p = _pipeline(window_hours=2, min_runs=3)
    result = check_velocity(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.exceeded is False
    assert result.actual_runs == 3


def test_too_few_runs_exceeded(store):
    store.append(_entry(30))
    p = _pipeline(window_hours=2, min_runs=3)
    result = check_velocity(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.exceeded is True
    assert result.actual_runs == 1


def test_entries_outside_window_excluded(store):
    store.append(_entry(30))
    store.append(_entry(200))  # outside 2-hour window
    p = _pipeline(window_hours=2, min_runs=2)
    result = check_velocity(p, store, now_fn=_now_fn)
    assert result.actual_runs == 1
    assert result.exceeded is True


def test_empty_history_exceeded(store):
    p = _pipeline(window_hours=24, min_runs=1)
    result = check_velocity(p, store, now_fn=_now_fn)
    assert result.exceeded is True
    assert result.actual_runs == 0


def test_summary_ok(store):
    for m in [10, 20, 30]:
        store.append(_entry(m))
    p = _pipeline(window_hours=1, min_runs=3)
    result = check_velocity(p, store, now_fn=_now_fn)
    assert "OK" in result.summary()
    assert "pipe" in result.summary()


def test_summary_low(store):
    p = _pipeline(window_hours=1, min_runs=5)
    result = check_velocity(p, store, now_fn=_now_fn)
    assert "LOW" in result.summary()


def test_check_all_skips_unconfigured(store):
    p1 = _pipeline("a", window_hours=1, min_runs=2)
    p2 = PipelineConfig(name="b", schedule_minutes=60, max_age_minutes=120)
    results = check_all_velocity([p1, p2], store, now_fn=_now_fn)
    assert len(results) == 1
    assert results[0].pipeline == "a"


def test_check_all_returns_all_configured(store):
    p1 = _pipeline("a", window_hours=1, min_runs=2)
    p2 = _pipeline("b", window_hours=2, min_runs=1)
    results = check_all_velocity([p1, p2], store, now_fn=_now_fn)
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}
