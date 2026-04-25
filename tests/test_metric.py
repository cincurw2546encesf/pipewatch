"""Tests for pipewatch.metric."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.metric import MetricStore


@pytest.fixture
def store(tmp_path: Path) -> MetricStore:
    return MetricStore(tmp_path / "metrics.json")


def test_empty_store_returns_no_entries(store: MetricStore) -> None:
    assert store.get("pipe_a") == []


def test_empty_store_summarise_returns_none(store: MetricStore) -> None:
    assert store.summarise("pipe_a") is None


def test_record_single_entry(store: MetricStore) -> None:
    entry = store.record("pipe_a", 42.5)
    assert entry.pipeline == "pipe_a"
    assert entry.duration_seconds == 42.5
    assert entry.recorded_at  # non-empty ISO string


def test_record_persists_across_reload(store: MetricStore, tmp_path: Path) -> None:
    store.record("pipe_a", 10.0)
    store2 = MetricStore(tmp_path / "metrics.json")
    entries = store2.get("pipe_a")
    assert len(entries) == 1
    assert entries[0].duration_seconds == 10.0


def test_get_filters_by_pipeline(store: MetricStore) -> None:
    store.record("pipe_a", 5.0)
    store.record("pipe_b", 15.0)
    store.record("pipe_a", 8.0)
    assert len(store.get("pipe_a")) == 2
    assert len(store.get("pipe_b")) == 1


def test_summarise_mean_min_max(store: MetricStore) -> None:
    for d in [10.0, 20.0, 30.0]:
        store.record("pipe_a", d)
    s = store.summarise("pipe_a")
    assert s is not None
    assert s.count == 3
    assert s.mean_seconds == pytest.approx(20.0)
    assert s.min_seconds == 10.0
    assert s.max_seconds == 30.0


def test_summarise_p95_single_entry(store: MetricStore) -> None:
    store.record("pipe_a", 99.0)
    s = store.summarise("pipe_a")
    assert s is not None
    assert s.p95_seconds == 99.0


def test_summarise_p95_many_entries(store: MetricStore) -> None:
    for i in range(1, 21):  # 1..20
        store.record("pipe_a", float(i))
    s = store.summarise("pipe_a")
    assert s is not None
    # p95 index = int(20*0.95)-1 = 18 -> sorted[18] = 19.0
    assert s.p95_seconds == pytest.approx(19.0)


def test_clear_removes_entries(store: MetricStore) -> None:
    store.record("pipe_a", 1.0)
    store.record("pipe_a", 2.0)
    store.record("pipe_b", 3.0)
    removed = store.clear("pipe_a")
    assert removed == 2
    assert store.get("pipe_a") == []
    assert len(store.get("pipe_b")) == 1


def test_summary_str_format(store: MetricStore) -> None:
    store.record("pipe_a", 5.0)
    s = store.summarise("pipe_a")
    assert s is not None
    text = str(s)
    assert "pipe_a" in text
    assert "mean=" in text
    assert "p95=" in text
