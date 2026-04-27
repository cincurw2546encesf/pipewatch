"""Integration tests: outlier detection using real HistoryStore on disk."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.outlier import check_all_outliers, check_outlier


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(str(history_file))


def _pipeline(name: str = "etl") -> PipelineConfig:
    return PipelineConfig(name=name, max_age_minutes=60)


def _add(store: HistoryStore, name: str, duration: float, minute: int) -> None:
    ts = datetime(2024, 3, 1, 10, minute, 0, tzinfo=timezone.utc).isoformat()
    store.append(HistoryEntry(
        pipeline=name,
        status="ok",
        checked_at=ts,
        last_run=ts,
        duration_seconds=duration,
    ))


def test_no_outlier_with_stable_durations(store: HistoryStore):
    for i, d in enumerate([60.0, 61.0, 59.5, 60.2, 60.8, 59.9, 60.1, 60.3]):
        _add(store, "etl", d, minute=i)
    result = check_outlier(_pipeline(), store, threshold=3.0)
    assert result.is_outlier is False


def test_outlier_detected_after_spike(store: HistoryStore):
    stable = [60.0] * 10
    for i, d in enumerate(stable):
        _add(store, "etl", d, minute=i + 1)
    # Most recent entry (minute=0 sorts last-appended first via limit)
    _add(store, "etl", 3600.0, minute=0)
    result = check_outlier(_pipeline(), store, threshold=2.0)
    assert result.is_outlier is True
    assert result.last_duration_seconds == pytest.approx(3600.0)


def test_multiple_pipelines_independent(store: HistoryStore):
    pipes = [_pipeline("a"), _pipeline("b")]
    # Pipeline a: stable
    for i, d in enumerate([10.0, 10.1, 9.9, 10.0, 10.2]):
        _add(store, "a", d, minute=i)
    # Pipeline b: only 1 entry — not enough for stats
    _add(store, "b", 999.0, minute=0)

    results = check_all_outliers(pipes, store, threshold=3.0)
    by_name = {r.pipeline: r for r in results}

    assert by_name["a"].is_outlier is False
    assert by_name["b"].is_outlier is False  # insufficient history
    assert by_name["b"].mean_seconds is None


def test_persisted_history_used_across_reload(history_file: Path):
    s1 = HistoryStore(str(history_file))
    for i, d in enumerate([20.0, 20.1, 19.9, 20.0, 20.2, 19.8, 20.3, 20.1]):
        _add(s1, "etl", d, minute=i + 1)
    _add(s1, "etl", 500.0, minute=0)

    # Reload from disk
    s2 = HistoryStore(str(history_file))
    result = check_outlier(_pipeline(), s2, threshold=2.0)
    assert result.is_outlier is True
