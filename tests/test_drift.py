"""Tests for pipewatch.drift."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.drift import check_drift, check_all_drift, _mean_interval, DriftResult
from pipewatch.history import HistoryStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(offset_seconds: float) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(str(history_file))


def _seed(store: HistoryStore, pipeline: str, intervals_seconds: list[float]) -> None:
    """Add synthetic history entries spaced by the given intervals."""
    from pipewatch.history import HistoryEntry
    from pipewatch.checker import CheckStatus

    t = _dt(0)
    raw = []
    for gap in intervals_seconds:
        t = t + timedelta(seconds=gap)
        raw.append(
            HistoryEntry(
                pipeline=pipeline,
                status=CheckStatus.OK,
                checked_at=t,
                last_run=None,
                message="ok",
            )
        )
    # Write directly via the internal list
    existing = store._load()
    existing.setdefault(pipeline, [])
    existing[pipeline].extend([e.to_dict() for e in raw])
    store._save(existing)


# ---------------------------------------------------------------------------
# _mean_interval
# ---------------------------------------------------------------------------

def test_mean_interval_empty():
    assert _mean_interval([]) is None


def test_mean_interval_single():
    assert _mean_interval([_dt(0)]) is None


def test_mean_interval_uniform():
    ts = [_dt(i * 3600) for i in range(5)]
    result = _mean_interval(ts)
    assert result == pytest.approx(3600.0)


def test_mean_interval_variable():
    ts = [_dt(0), _dt(100), _dt(300)]
    result = _mean_interval(ts)
    assert result == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# check_drift
# ---------------------------------------------------------------------------

def test_check_drift_insufficient_history(store: HistoryStore):
    result = check_drift("pipe_a", 3600.0, store, min_entries=3)
    assert result.exceeded is False
    assert result.actual_interval_seconds is None
    assert "insufficient" in result.message


def test_check_drift_within_tolerance(store: HistoryStore, history_file: Path):
    _seed(store, "pipe_a", [3600, 3600, 3600, 3600])
    store2 = HistoryStore(str(history_file))
    result = check_drift("pipe_a", 3600.0, store2, tolerance=0.5)
    assert result.exceeded is False
    assert result.drift_ratio == pytest.approx(1.0, abs=0.01)


def test_check_drift_exceeded(store: HistoryStore, history_file: Path):
    # actual ~7200s vs expected 3600s => ratio 2.0, drift 100% > 50% tolerance
    _seed(store, "pipe_b", [7200, 7200, 7200, 7200])
    store2 = HistoryStore(str(history_file))
    result = check_drift("pipe_b", 3600.0, store2, tolerance=0.5)
    assert result.exceeded is True
    assert result.drift_ratio == pytest.approx(2.0, abs=0.05)
    assert "drift" in result.message


def test_check_drift_faster_than_expected(store: HistoryStore, history_file: Path):
    _seed(store, "pipe_c", [600, 600, 600, 600])
    store2 = HistoryStore(str(history_file))
    result = check_drift("pipe_c", 3600.0, store2, tolerance=0.5)
    assert result.exceeded is True
    assert result.drift_ratio == pytest.approx(600 / 3600, abs=0.05)


# ---------------------------------------------------------------------------
# check_all_drift
# ---------------------------------------------------------------------------

def test_check_all_drift_skips_pipelines_without_interval(store: HistoryStore):
    from types import SimpleNamespace
    pipelines = {"pipe_x": SimpleNamespace()}
    results = check_all_drift(pipelines, store)
    assert results == []


def test_check_all_drift_processes_configured_pipelines(
    store: HistoryStore, history_file: Path
):
    from types import SimpleNamespace
    _seed(store, "pipe_y", [3600, 3600, 3600, 3600])
    store2 = HistoryStore(str(history_file))
    pipelines = {"pipe_y": SimpleNamespace(expected_interval_seconds=3600.0)}
    results = check_all_drift(pipelines, store2)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_y"
