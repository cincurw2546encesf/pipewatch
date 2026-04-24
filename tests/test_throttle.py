"""Tests for pipewatch.throttle."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.throttle import ThrottleStore


@pytest.fixture()
def store(tmp_path: Path) -> ThrottleStore:
    return ThrottleStore(tmp_path / "throttle.json")


def test_empty_store_not_throttled(store: ThrottleStore) -> None:
    assert store.is_throttled("pipe_a", cooldown_seconds=300) is False


def test_record_alert_marks_throttled(store: ThrottleStore) -> None:
    store.record_alert("pipe_a")
    assert store.is_throttled("pipe_a", cooldown_seconds=300) is True


def test_throttle_expires_after_cooldown(store: ThrottleStore, tmp_path: Path) -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=400)
    store.record_alert("pipe_a")
    # Manually backdate the entry
    entry = store.get("pipe_a")
    assert entry is not None
    entry.last_alerted = past
    store._entries["pipe_a"] = entry
    store._save()

    fresh = ThrottleStore(tmp_path / "throttle.json")
    assert fresh.is_throttled("pipe_a", cooldown_seconds=300) is False


def test_reset_removes_throttle(store: ThrottleStore) -> None:
    store.record_alert("pipe_a")
    store.reset("pipe_a")
    assert store.is_throttled("pipe_a", cooldown_seconds=300) is False
    assert store.get("pipe_a") is None


def test_alert_count_increments(store: ThrottleStore) -> None:
    store.record_alert("pipe_a")
    store.record_alert("pipe_a")
    entry = store.get("pipe_a")
    assert entry is not None
    assert entry.alert_count == 2


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "throttle.json"
    s1 = ThrottleStore(path)
    s1.record_alert("pipe_b")

    s2 = ThrottleStore(path)
    assert s2.is_throttled("pipe_b", cooldown_seconds=300) is True
    entry = s2.get("pipe_b")
    assert entry is not None
    assert entry.alert_count == 1


def test_multiple_pipelines_independent(store: ThrottleStore) -> None:
    store.record_alert("pipe_a")
    assert store.is_throttled("pipe_a", cooldown_seconds=300) is True
    assert store.is_throttled("pipe_b", cooldown_seconds=300) is False


def test_all_entries_returns_all(store: ThrottleStore) -> None:
    store.record_alert("pipe_a")
    store.record_alert("pipe_b")
    names = {e.pipeline for e in store.all_entries()}
    assert names == {"pipe_a", "pipe_b"}
