"""Tests for pipewatch.burst."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.burst import BurstResult, check_burst, check_all_burst
from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.checker import CheckStatus


UTC = timezone.utc


def _dt(offset_seconds: float = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC) + timedelta(seconds=offset_seconds)


NOW = _dt(0)


def _now_fn():
    return NOW


class _Pipeline:
    def __init__(self, name="pipe", burst_max_runs=None, burst_window_seconds=None):
        self.name = name
        self.burst_max_runs = burst_max_runs
        self.burst_window_seconds = burst_window_seconds


@pytest.fixture()
def history_file(tmp_path):
    return str(tmp_path / "history.json")


@pytest.fixture()
def store(history_file):
    return HistoryStore(history_file)


def _add(store: HistoryStore, name: str, finished_at: datetime) -> None:
    entry = HistoryEntry(
        pipeline=name,
        status=CheckStatus.OK,
        started_at=finished_at - timedelta(seconds=5),
        finished_at=finished_at,
        duration_seconds=5.0,
    )
    store.append(entry)


def test_no_burst_config_returns_none(store):
    p = _Pipeline("pipe")  # no burst attrs
    result = check_burst(p, store, now_fn=_now_fn)
    assert result is None


def test_no_runs_in_window_not_exceeded(store):
    p = _Pipeline("pipe", burst_max_runs=3, burst_window_seconds=60)
    result = check_burst(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.run_count == 0
    assert not result.exceeded


def test_runs_within_limit_not_exceeded(store):
    p = _Pipeline("pipe", burst_max_runs=3, burst_window_seconds=60)
    for offset in [-50, -30, -10]:
        _add(store, "pipe", _dt(offset))
    result = check_burst(p, store, now_fn=_now_fn)
    assert result.run_count == 3
    assert not result.exceeded


def test_runs_exceed_limit(store):
    p = _Pipeline("pipe", burst_max_runs=2, burst_window_seconds=60)
    for offset in [-50, -30, -10]:
        _add(store, "pipe", _dt(offset))
    result = check_burst(p, store, now_fn=_now_fn)
    assert result.run_count == 3
    assert result.exceeded


def test_runs_outside_window_excluded(store):
    p = _Pipeline("pipe", burst_max_runs=2, burst_window_seconds=60)
    _add(store, "pipe", _dt(-120))  # outside 60s window
    _add(store, "pipe", _dt(-30))   # inside
    result = check_burst(p, store, now_fn=_now_fn)
    assert result.run_count == 1
    assert not result.exceeded


def test_summary_exceeded(store):
    p = _Pipeline("pipe", burst_max_runs=1, burst_window_seconds=30)
    _add(store, "pipe", _dt(-10))
    _add(store, "pipe", _dt(-5))
    result = check_burst(p, store, now_fn=_now_fn)
    assert "BURST" in result.summary()


def test_summary_ok(store):
    p = _Pipeline("pipe", burst_max_runs=5, burst_window_seconds=60)
    _add(store, "pipe", _dt(-10))
    result = check_burst(p, store, now_fn=_now_fn)
    assert "OK" in result.summary()


def test_check_all_burst_skips_no_config(store):
    pipelines = [
        _Pipeline("a"),  # no burst config
        _Pipeline("b", burst_max_runs=2, burst_window_seconds=60),
    ]
    results = check_all_burst(pipelines, store, now_fn=_now_fn)
    assert len(results) == 1
    assert results[0].pipeline == "b"


def test_oldest_newest_populated(store):
    p = _Pipeline("pipe", burst_max_runs=5, burst_window_seconds=120)
    t1, t2 = _dt(-90), _dt(-10)
    _add(store, "pipe", t1)
    _add(store, "pipe", t2)
    result = check_burst(p, store, now_fn=_now_fn)
    assert result.oldest_in_window == t1
    assert result.newest_in_window == t2
