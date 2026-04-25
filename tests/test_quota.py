"""Tests for pipewatch.quota."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.quota import QuotaEntry, QuotaStore


@pytest.fixture()
def store(tmp_path: Path) -> QuotaStore:
    return QuotaStore(tmp_path / "quota.json")


def _dt(hours_ago: float = 0) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours_ago)


def _now_fn(dt: datetime):
    return lambda: dt


# --- QuotaEntry unit tests ---

def test_entry_no_failures_not_exceeded():
    entry = QuotaEntry(pipeline="p", max_failures=3, window_hours=24)
    assert not entry.exceeded()
    assert entry.failures_in_window() == 0


def test_entry_record_failure_increments():
    entry = QuotaEntry(pipeline="p", max_failures=3, window_hours=24)
    entry.record_failure()
    assert entry.failures_in_window() == 1


def test_entry_exceeds_at_max():
    entry = QuotaEntry(pipeline="p", max_failures=2, window_hours=24)
    entry.record_failure()
    entry.record_failure()
    assert entry.exceeded()


def test_entry_prunes_old_timestamps():
    entry = QuotaEntry(pipeline="p", max_failures=3, window_hours=1)
    old = _dt(hours_ago=2)
    entry.failure_timestamps = [old.isoformat()]
    assert entry.failures_in_window() == 0


def test_entry_does_not_prune_recent_timestamps():
    entry = QuotaEntry(pipeline="p", max_failures=3, window_hours=24)
    recent = _dt(hours_ago=0.5)
    entry.failure_timestamps = [recent.isoformat()]
    assert entry.failures_in_window() == 1


def test_entry_to_dict_round_trip():
    entry = QuotaEntry(pipeline="p", max_failures=5, window_hours=12)
    entry.record_failure()
    restored = QuotaEntry.from_dict(entry.to_dict())
    assert restored.pipeline == "p"
    assert restored.max_failures == 5
    assert restored.window_hours == 12
    assert len(restored.failure_timestamps) == 1


# --- QuotaStore tests ---

def test_empty_store_returns_none(store: QuotaStore):
    assert store.get("missing") is None


def test_configure_creates_entry(store: QuotaStore):
    entry = store.configure("etl", max_failures=3, window_hours=24)
    assert entry.pipeline == "etl"
    assert entry.max_failures == 3


def test_configure_updates_existing(store: QuotaStore):
    store.configure("etl", max_failures=3, window_hours=24)
    updated = store.configure("etl", max_failures=10, window_hours=48)
    assert updated.max_failures == 10
    assert updated.window_hours == 48


def test_record_failure_returns_none_for_unconfigured(store: QuotaStore):
    result = store.record_failure("unknown")
    assert result is None


def test_record_failure_increments(store: QuotaStore):
    store.configure("etl", max_failures=5, window_hours=24)
    entry = store.record_failure("etl")
    assert entry is not None
    assert entry.failures_in_window() == 1


def test_record_failure_persists(tmp_path: Path):
    path = tmp_path / "quota.json"
    s1 = QuotaStore(path)
    s1.configure("etl", max_failures=5, window_hours=24)
    s1.record_failure("etl")
    s2 = QuotaStore(path)
    assert s2.get("etl").failures_in_window() == 1


def test_reset_clears_failures(store: QuotaStore):
    store.configure("etl", max_failures=3, window_hours=24)
    store.record_failure("etl")
    store.reset("etl")
    assert store.get("etl").failures_in_window() == 0


def test_reset_returns_false_for_unknown(store: QuotaStore):
    assert store.reset("ghost") is False


def test_all_returns_all_entries(store: QuotaStore):
    store.configure("a", 3, 24)
    store.configure("b", 5, 12)
    names = {e.pipeline for e in store.all()}
    assert names == {"a", "b"}
