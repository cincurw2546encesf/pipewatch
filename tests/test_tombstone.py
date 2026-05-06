"""Tests for pipewatch.tombstone."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.tombstone import TombstoneStore


@pytest.fixture()
def store(tmp_path: Path) -> TombstoneStore:
    return TombstoneStore(tmp_path / "tombstones.json")


def test_empty_store_not_retired(store: TombstoneStore) -> None:
    assert store.is_retired("my_pipeline") is False


def test_empty_store_get_returns_none(store: TombstoneStore) -> None:
    assert store.get("my_pipeline") is None


def test_empty_store_all_returns_empty(store: TombstoneStore) -> None:
    assert store.all() == []


def test_retire_marks_pipeline(store: TombstoneStore) -> None:
    store.retire("pipe_a", reason="Replaced by pipe_b")
    assert store.is_retired("pipe_a") is True


def test_retire_returns_entry(store: TombstoneStore) -> None:
    entry = store.retire("pipe_a", reason="Decommissioned", retired_by="alice")
    assert entry.pipeline == "pipe_a"
    assert entry.reason == "Decommissioned"
    assert entry.retired_by == "alice"
    assert isinstance(entry.retired_at, datetime)


def test_retire_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "tombstones.json"
    s1 = TombstoneStore(path)
    s1.retire("pipe_a", reason="Old pipeline")
    s2 = TombstoneStore(path)
    assert s2.is_retired("pipe_a") is True


def test_restore_removes_tombstone(store: TombstoneStore) -> None:
    store.retire("pipe_a", reason="test")
    result = store.restore("pipe_a")
    assert result is True
    assert store.is_retired("pipe_a") is False


def test_restore_unknown_returns_false(store: TombstoneStore) -> None:
    assert store.restore("nonexistent") is False


def test_restore_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "tombstones.json"
    s1 = TombstoneStore(path)
    s1.retire("pipe_a", reason="test")
    s1.restore("pipe_a")
    s2 = TombstoneStore(path)
    assert s2.is_retired("pipe_a") is False


def test_get_returns_entry(store: TombstoneStore) -> None:
    store.retire("pipe_a", reason="Sunset", retired_by="bob")
    entry = store.get("pipe_a")
    assert entry is not None
    assert entry.pipeline == "pipe_a"
    assert entry.retired_by == "bob"


def test_summary_contains_fields(store: TombstoneStore) -> None:
    entry = store.retire("pipe_a", reason="No longer needed", retired_by="carol")
    s = entry.summary()
    assert "pipe_a" in s
    assert "carol" in s
    assert "No longer needed" in s


def test_summary_without_retired_by(store: TombstoneStore) -> None:
    entry = store.retire("pipe_b", reason="Archived")
    s = entry.summary()
    assert "pipe_b" in s
    assert "Archived" in s
    assert "by" not in s


def test_all_returns_multiple_entries(store: TombstoneStore) -> None:
    store.retire("pipe_a", reason="A")
    store.retire("pipe_b", reason="B")
    names = {e.pipeline for e in store.all()}
    assert names == {"pipe_a", "pipe_b"}
