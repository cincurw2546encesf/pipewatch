"""Tests for pipewatch.outlier."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.outlier import (
    OutlierResult,
    _compute_stats,
    check_all_outliers,
    check_outlier,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(str(history_file))


def _pipeline(name: str = "pipe") -> PipelineConfig:
    return PipelineConfig(name=name, max_age_minutes=60)


def _dt(offset: int = 0) -> datetime:
    return datetime(2024, 1, 10, 12, offset, 0, tzinfo=timezone.utc)


def _add(store: HistoryStore, name: str, duration: float, offset: int = 0) -> None:
    entry = HistoryEntry(
        pipeline=name,
        status="ok",
        checked_at=_dt(offset).isoformat(),
        last_run=_dt(offset).isoformat(),
        duration_seconds=duration,
    )
    store.append(entry)


# ---------------------------------------------------------------------------
# _compute_stats
# ---------------------------------------------------------------------------

def test_compute_stats_empty():
    mean, stddev = _compute_stats([])
    assert mean is None
    assert stddev is None


def test_compute_stats_single():
    mean, stddev = _compute_stats([42.0])
    assert mean is None
    assert stddev is None


def test_compute_stats_two_values():
    mean, stddev = _compute_stats([10.0, 20.0])
    assert mean == pytest.approx(15.0)
    assert stddev == pytest.approx(math.sqrt(50.0))


# ---------------------------------------------------------------------------
# check_outlier
# ---------------------------------------------------------------------------

def test_check_outlier_no_history(store: HistoryStore):
    result = check_outlier(_pipeline(), store)
    assert result.is_outlier is False
    assert result.last_duration_seconds is None
    assert result.z_score is None


def test_check_outlier_insufficient_history(store: HistoryStore):
    _add(store, "pipe", 10.0, offset=0)
    result = check_outlier(_pipeline(), store)
    assert result.is_outlier is False
    assert result.mean_seconds is None


def test_check_outlier_not_outlier(store: HistoryStore):
    for i, d in enumerate([10.0, 11.0, 10.5, 10.2, 10.8, 11.1]):
        _add(store, "pipe", d, offset=i)
    result = check_outlier(_pipeline(), store, threshold=3.0)
    assert result.is_outlier is False
    assert result.z_score is not None


def test_check_outlier_flags_outlier(store: HistoryStore):
    # Seed with very consistent values, then add a huge spike as most recent
    for i, d in enumerate([10.0, 10.1, 9.9, 10.0, 10.1, 9.8, 10.2, 10.0, 9.9, 10.1]):
        _add(store, "pipe", d, offset=i + 1)
    _add(store, "pipe", 500.0, offset=0)  # most recent — offset 0 = first returned
    result = check_outlier(_pipeline(), store, threshold=3.0)
    assert result.is_outlier is True
    assert result.z_score is not None
    assert abs(result.z_score) > 3.0


def test_check_outlier_zero_stddev(store: HistoryStore):
    for i in range(5):
        _add(store, "pipe", 10.0, offset=i)
    result = check_outlier(_pipeline(), store, threshold=3.0)
    assert result.is_outlier is False
    assert result.z_score == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# check_all_outliers
# ---------------------------------------------------------------------------

def test_check_all_outliers_returns_one_per_pipeline(store: HistoryStore):
    pipes = [_pipeline("a"), _pipeline("b"), _pipeline("c")]
    results = check_all_outliers(pipes, store)
    assert len(results) == 3
    assert {r.pipeline for r in results} == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# OutlierResult.summary
# ---------------------------------------------------------------------------

def test_summary_no_data():
    r = OutlierResult(
        pipeline="p", mean_seconds=None, stddev_seconds=None,
        last_duration_seconds=None, z_score=None, is_outlier=False, threshold=3.0,
    )
    assert "no recent duration" in r.summary()


def test_summary_outlier_flag():
    r = OutlierResult(
        pipeline="p", mean_seconds=10.0, stddev_seconds=1.0,
        last_duration_seconds=50.0, z_score=40.0, is_outlier=True, threshold=3.0,
    )
    assert "[OUTLIER]" in r.summary()


def test_summary_normal_no_flag():
    r = OutlierResult(
        pipeline="p", mean_seconds=10.0, stddev_seconds=1.0,
        last_duration_seconds=10.5, z_score=0.5, is_outlier=False, threshold=3.0,
    )
    assert "[OUTLIER]" not in r.summary()
