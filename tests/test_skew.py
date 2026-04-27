"""Tests for pipewatch.skew."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.skew import SkewResult, check_skew, check_all_skew


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 15, hour, minute, 0, tzinfo=timezone.utc)


class _Pipeline:
    def __init__(self, name="pipe", expected_hour=None, max_skew_minutes=None):
        self.name = name
        self.expected_hour = expected_hour
        self.max_skew_minutes = max_skew_minutes


def _make_store(tmp_path, entries: List[HistoryEntry]):
    store = HistoryStore(str(tmp_path / "history.json"))
    for e in entries:
        store.append(e)
    return store


def _entry(name: str, started_at: datetime) -> HistoryEntry:
    return HistoryEntry(
        pipeline=name,
        status="ok",
        checked_at=started_at,
        started_at=started_at,
        finished_at=None,
        duration_seconds=None,
    )


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_no_config_returns_none(tmp_path):
    store = _make_store(tmp_path, [])
    p = _Pipeline("p1")  # no expected_hour / max_skew_minutes
    assert check_skew(p, store) is None


def test_insufficient_history_returns_result_not_exceeded(tmp_path):
    store = _make_store(tmp_path, [
        _entry("p1", _dt(6, 0)),
        _entry("p1", _dt(6, 5)),
    ])
    p = _Pipeline("p1", expected_hour=6, max_skew_minutes=30)
    result = check_skew(p, store, min_entries=3)
    assert result is not None
    assert result.actual_avg_hour is None
    assert result.skew_minutes is None
    assert not result.exceeded


def test_on_time_pipeline_not_exceeded(tmp_path):
    store = _make_store(tmp_path, [
        _entry("p1", _dt(6, 0)),
        _entry("p1", _dt(6, 2)),
        _entry("p1", _dt(6, 1)),
    ])
    p = _Pipeline("p1", expected_hour=6, max_skew_minutes=30)
    result = check_skew(p, store, min_entries=3)
    assert result is not None
    assert not result.exceeded
    assert result.skew_minutes is not None
    assert result.skew_minutes < 5  # tiny deviation


def test_late_pipeline_exceeded(tmp_path):
    # pipeline consistently runs at 08:00, expected at 06:00 → 120 min skew
    store = _make_store(tmp_path, [
        _entry("p1", _dt(8, 0)),
        _entry("p1", _dt(8, 0)),
        _entry("p1", _dt(8, 0)),
    ])
    p = _Pipeline("p1", expected_hour=6, max_skew_minutes=30)
    result = check_skew(p, store, min_entries=3)
    assert result is not None
    assert result.exceeded
    assert result.skew_minutes == pytest.approx(120.0, abs=0.1)


def test_summary_ok(tmp_path):
    store = _make_store(tmp_path, [
        _entry("p1", _dt(6, 0)),
        _entry("p1", _dt(6, 0)),
        _entry("p1", _dt(6, 0)),
    ])
    p = _Pipeline("p1", expected_hour=6, max_skew_minutes=30)
    result = check_skew(p, store, min_entries=3)
    assert "OK" in result.summary()
    assert "p1" in result.summary()


def test_summary_skewed(tmp_path):
    store = _make_store(tmp_path, [
        _entry("p1", _dt(10, 0)),
        _entry("p1", _dt(10, 0)),
        _entry("p1", _dt(10, 0)),
    ])
    p = _Pipeline("p1", expected_hour=6, max_skew_minutes=30)
    result = check_skew(p, store, min_entries=3)
    assert "SKEWED" in result.summary()


def test_check_all_skew_filters_none(tmp_path):
    store = _make_store(tmp_path, [])
    pipelines = [
        _Pipeline("no_config"),           # returns None
        _Pipeline("p2", 6, 30),           # returns SkewResult
    ]
    results = check_all_skew(pipelines, store)
    assert len(results) == 1
    assert results[0].pipeline == "p2"
