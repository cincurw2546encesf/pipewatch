"""Integration tests: capacity check using real HistoryStore and PipelineConfig."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.capacity import check_capacity, check_all_capacity
from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.config import PipelineConfig


@pytest.fixture()
def history_file(tmp_path):
    return str(tmp_path / "history.json")


@pytest.fixture()
def store(history_file):
    return HistoryStore(history_file)


def _pipeline(name: str, budget: float) -> PipelineConfig:
    p = PipelineConfig(name=name, max_age_minutes=60)
    object.__setattr__(p, "budget_seconds", budget)
    return p


def _add(store, name, duration, offset=0):
    now = datetime.now(timezone.utc) - timedelta(minutes=offset)
    store.append(HistoryEntry(
        pipeline=name,
        status="ok",
        checked_at=now.isoformat(),
        last_run=now.isoformat(),
        duration_seconds=duration,
    ))


def test_no_outlier_with_stable_durations(store):
    p = _pipeline("stable", budget_seconds=200.0)
    for i in range(8):
        _add(store, "stable", 100.0, offset=i)
    result = check_capacity(p, store)
    assert not result.exceeded
    assert result.utilisation == pytest.approx(0.5)


def test_all_pipelines_checked(store):
    pipelines = [_pipeline("a", 60.0), _pipeline("b", 60.0)]
    _add(store, "a", 30.0)
    _add(store, "b", 30.0)
    results = check_all_capacity(pipelines, store)
    assert len(results) == 2
    assert all(not r.exceeded for r in results)


def test_persists_across_store_reload(history_file):
    store1 = HistoryStore(history_file)
    p = _pipeline("reload_test", budget_seconds=100.0)
    for i in range(5):
        _add(store1, "reload_test", 80.0, offset=i)

    store2 = HistoryStore(history_file)
    result = check_capacity(p, store2)
    assert result.sample_count == 5
    assert result.avg_duration_seconds == pytest.approx(80.0)
