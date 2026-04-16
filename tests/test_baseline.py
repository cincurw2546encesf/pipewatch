"""Tests for baseline store and baseline checker."""
from __future__ import annotations
import pytest
from pathlib import Path
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta

from pipewatch.baseline import BaselineStore, BaselineEntry
from pipewatch.baseline_checker import check_baseline, BaselineCheckResult


@pytest.fixture
def store(tmp_path: Path) -> BaselineStore:
    return BaselineStore(tmp_path / "baselines.json")


def test_empty_store_returns_none(store: BaselineStore) -> None:
    assert store.get("pipe_a") is None


def test_update_creates_entry(store: BaselineStore) -> None:
    entry = store.update("pipe_a", 30.0)
    assert entry.pipeline == "pipe_a"
    assert entry.avg_duration_seconds == 30.0
    assert entry.sample_count == 1


def test_update_averages(store: BaselineStore) -> None:
    store.update("pipe_a", 20.0)
    entry = store.update("pipe_a", 40.0)
    assert entry.avg_duration_seconds == 30.0
    assert entry.sample_count == 2


def test_persists_across_reload(tmp_path: Path) -> None:
    p = tmp_path / "baselines.json"
    s1 = BaselineStore(p)
    s1.update("pipe_b", 60.0)
    s2 = BaselineStore(p)
    assert s2.get("pipe_b") is not None
    assert s2.get("pipe_b").avg_duration_seconds == 60.0


def test_remove_existing(store: BaselineStore) -> None:
    store.update("pipe_a", 10.0)
    assert store.remove("pipe_a") is True
    assert store.get("pipe_a") is None


def test_remove_missing(store: BaselineStore) -> None:
    assert store.remove("ghost") is False


def test_check_baseline_no_baseline(store: BaselineStore) -> None:
    hs = MagicMock()
    hs.get.return_value = []
    result = check_baseline("pipe_a", store, hs)
    assert result.baseline is None
    assert not result.exceeded_threshold


def test_check_baseline_exceeded(store: BaselineStore) -> None:
    store.update("pipe_a", 10.0)
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    entry = MagicMock()
    entry.started_at = now
    entry.finished_at = now + timedelta(seconds=25)  # 150% of 10s
    hs = MagicMock()
    hs.get.return_value = [entry]
    result = check_baseline("pipe_a", store, hs, threshold_pct=50.0)
    assert result.exceeded_threshold is True
    assert result.deviation_pct == 150.0


def test_check_baseline_within_threshold(store: BaselineStore) -> None:
    store.update("pipe_a", 20.0)
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    entry = MagicMock()
    entry.started_at = now
    entry.finished_at = now + timedelta(seconds=22)
    hs = MagicMock()
    hs.get.return_value = [entry]
    result = check_baseline("pipe_a", store, hs, threshold_pct=50.0)
    assert not result.exceeded_threshold
