"""Tests for pipewatch.capacity."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest

from pipewatch.capacity import check_capacity, check_all_capacity, CapacityResult
from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.config import PipelineConfig


@pytest.fixture()
def history_file(tmp_path):
    return str(tmp_path / "history.json")


@pytest.fixture()
def store(history_file):
    return HistoryStore(history_file)


def _pipeline(name: str = "etl", budget_seconds: Optional[float] = None) -> PipelineConfig:
    p = PipelineConfig(name=name, max_age_minutes=60)
    if budget_seconds is not None:
        object.__setattr__(p, "budget_seconds", budget_seconds)
    return p


def _add(store: HistoryStore, pipeline: str, duration: float, offset_minutes: int = 0) -> None:
    now = datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)
    entry = HistoryEntry(
        pipeline=pipeline,
        status="ok",
        checked_at=now.isoformat(),
        last_run=now.isoformat(),
        duration_seconds=duration,
    )
    store.append(entry)


# --- unit tests ---

def test_no_history_returns_none_avg(store):
    p = _pipeline("p1", budget_seconds=120.0)
    result = check_capacity(p, store)
    assert result.avg_duration_seconds is None
    assert result.exceeded is False
    assert result.sample_count == 0


def test_within_budget(store):
    p = _pipeline("p1", budget_seconds=120.0)
    for i in range(5):
        _add(store, "p1", 60.0, offset_minutes=i)
    result = check_capacity(p, store)
    assert result.avg_duration_seconds == pytest.approx(60.0)
    assert result.utilisation == pytest.approx(0.5)
    assert result.exceeded is False


def test_exceeded_budget(store):
    p = _pipeline("p1", budget_seconds=50.0)
    for i in range(5):
        _add(store, "p1", 100.0, offset_minutes=i)
    result = check_capacity(p, store)
    assert result.exceeded is True
    assert result.utilisation == pytest.approx(2.0)


def test_no_budget_configured(store):
    p = _pipeline("p1")  # no budget_seconds attr
    _add(store, "p1", 90.0)
    result = check_capacity(p, store)
    assert result.budget_seconds is None
    assert result.utilisation is None
    assert result.exceeded is False


def test_window_limits_samples(store):
    p = _pipeline("p1", budget_seconds=200.0)
    # Add 20 runs: 10 fast, then 10 slow
    for i in range(10):
        _add(store, "p1", 10.0, offset_minutes=20 + i)
    for i in range(10):
        _add(store, "p1", 190.0, offset_minutes=i)
    result = check_capacity(p, store, window=10)
    # Only the 10 most recent (slow) runs should be averaged
    assert result.avg_duration_seconds == pytest.approx(190.0)
    assert result.sample_count == 10


def test_check_all_capacity_returns_one_per_pipeline(store):
    pipelines = [_pipeline("a", 60.0), _pipeline("b", 120.0)]
    _add(store, "a", 30.0)
    results = check_all_capacity(pipelines, store)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}


def test_summary_no_data(store):
    p = _pipeline("p1", budget_seconds=100.0)
    result = check_capacity(p, store)
    assert "no duration data" in result.summary()


def test_summary_exceeded(store):
    p = _pipeline("p1", budget_seconds=50.0)
    _add(store, "p1", 100.0)
    result = check_capacity(p, store)
    assert "EXCEEDED" in result.summary()
