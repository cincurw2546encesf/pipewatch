"""Tests for pipewatch.lockout."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.lockout import LockoutEntry, LockoutStore


@pytest.fixture
def store(tmp_path: Path) -> LockoutStore:
    return LockoutStore(tmp_path / "lockout.json")


def _dt(offset_seconds: float = 0.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)


def test_empty_store_not_locked(store: LockoutStore) -> None:
    assert not store.is_locked("my_pipeline")


def test_lock_pipeline(store: LockoutStore) -> None:
    store.lock("pipe_a", duration_seconds=300, reason="too many failures")
    assert store.is_locked("pipe_a")


def test_lock_entry_has_reason(store: LockoutStore) -> None:
    store.lock("pipe_b", duration_seconds=60, reason="manual lockout")
    entry = store.get("pipe_b")
    assert entry is not None
    assert entry.reason == "manual lockout"


def test_lock_expires(store: LockoutStore) -> None:
    past = _dt(-1)
    entry = LockoutEntry(pipeline="pipe_c", locked_until=past)
    assert not entry.is_locked()


def test_seconds_remaining_positive(store: LockoutStore) -> None:
    store.lock("pipe_d", duration_seconds=120)
    entry = store.get("pipe_d")
    assert entry is not None
    remaining = entry.seconds_remaining()
    assert 0 < remaining <= 120


def test_seconds_remaining_zero_when_expired() -> None:
    past = _dt(-100)
    entry = LockoutEntry(pipeline="pipe_e", locked_until=past)
    assert entry.seconds_remaining() == 0.0


def test_unlock_removes_entry(store: LockoutStore) -> None:
    store.lock("pipe_f", duration_seconds=60)
    assert store.is_locked("pipe_f")
    removed = store.unlock("pipe_f")
    assert removed
    assert not store.is_locked("pipe_f")


def test_unlock_nonexistent_returns_false(store: LockoutStore) -> None:
    assert not store.unlock("ghost_pipeline")


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "lockout.json"
    s1 = LockoutStore(path)
    s1.lock("pipe_g", duration_seconds=500)
    s2 = LockoutStore(path)
    assert s2.is_locked("pipe_g")


def test_all_entries_returns_list(store: LockoutStore) -> None:
    store.lock("pipe_h", duration_seconds=60)
    store.lock("pipe_i", duration_seconds=120)
    entries = store.all_entries()
    names = {e.pipeline for e in entries}
    assert "pipe_h" in names
    assert "pipe_i" in names


def test_get_unknown_returns_none(store: LockoutStore) -> None:
    assert store.get("nonexistent") is None
