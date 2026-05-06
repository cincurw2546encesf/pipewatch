"""Tests for pipewatch.drain."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.drain import check_drain, check_all_drain, DrainResult
from pipewatch.history import HistoryStore, HistoryEntry


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _dt(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)


class _Pipeline:
    def __init__(self, name: str, min_duration_seconds=None):
        self.name = name
        self.min_duration_seconds = min_duration_seconds


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(history_file)


def _add(store: HistoryStore, pipeline: str, duration: float):
    entry = HistoryEntry(
        pipeline=pipeline,
        status="ok",
        checked_at=_dt().isoformat(),
        last_run=_dt().isoformat(),
        duration_seconds=duration,
    )
    store.append(pipeline, entry)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_no_min_duration_returns_none(store):
    p = _Pipeline("pipe_a", min_duration_seconds=None)
    result = check_drain(p, store)
    assert result is None


def test_no_history_returns_not_draining(store):
    p = _Pipeline("pipe_a", min_duration_seconds=5.0)
    result = check_drain(p, store)
    assert result is not None
    assert result.is_draining is False
    assert result.run_count == 0


def test_avg_above_min_not_draining(store):
    p = _Pipeline("pipe_a", min_duration_seconds=5.0)
    for _ in range(5):
        _add(store, "pipe_a", 10.0)
    result = check_drain(p, store)
    assert result.is_draining is False
    assert abs(result.avg_duration_seconds - 10.0) < 0.01


def test_avg_below_min_is_draining(store):
    p = _Pipeline("pipe_a", min_duration_seconds=5.0)
    for _ in range(5):
        _add(store, "pipe_a", 1.0)
    result = check_drain(p, store)
    assert result.is_draining is True
    assert result.avg_duration_seconds < result.min_expected_seconds


def test_exactly_at_min_not_draining(store):
    p = _Pipeline("pipe_a", min_duration_seconds=3.0)
    for _ in range(4):
        _add(store, "pipe_a", 3.0)
    result = check_drain(p, store)
    assert result.is_draining is False


def test_window_limits_entries_considered(store):
    p = _Pipeline("pipe_a", min_duration_seconds=5.0)
    # older runs: long duration
    for _ in range(8):
        _add(store, "pipe_a", 20.0)
    # recent runs: short duration (should trigger drain with window=3)
    for _ in range(3):
        _add(store, "pipe_a", 1.0)
    result = check_drain(p, store, window=3)
    assert result.is_draining is True
    assert result.run_count == 3


def test_summary_draining_contains_keyword(store):
    p = _Pipeline("pipe_a", min_duration_seconds=5.0)
    for _ in range(3):
        _add(store, "pipe_a", 0.5)
    result = check_drain(p, store)
    assert "draining" in result.summary()


def test_summary_ok_contains_ok(store):
    p = _Pipeline("pipe_a", min_duration_seconds=2.0)
    for _ in range(3):
        _add(store, "pipe_a", 10.0)
    result = check_drain(p, store)
    assert "ok" in result.summary()


def test_check_all_drain_skips_unconfigured(store):
    pipelines = [
        _Pipeline("pipe_a", min_duration_seconds=None),
        _Pipeline("pipe_b", min_duration_seconds=5.0),
    ]
    results = check_all_drain(pipelines, store)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_b"


def test_check_all_drain_multiple_pipelines(store):
    pipelines = [
        _Pipeline("pipe_a", min_duration_seconds=5.0),
        _Pipeline("pipe_b", min_duration_seconds=5.0),
    ]
    for _ in range(3):
        _add(store, "pipe_a", 1.0)
        _add(store, "pipe_b", 10.0)
    results = check_all_drain(pipelines, store)
    by_name = {r.pipeline: r for r in results}
    assert by_name["pipe_a"].is_draining is True
    assert by_name["pipe_b"].is_draining is False
