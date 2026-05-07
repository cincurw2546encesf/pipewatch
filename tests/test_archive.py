"""Tests for pipewatch.archive."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.archive import ArchiveEntry, ArchiveStore


@pytest.fixture
def store(tmp_path: Path) -> ArchiveStore:
    return ArchiveStore(tmp_path / "archive.json")


def test_empty_store_not_archived(store: ArchiveStore) -> None:
    assert store.is_archived("pipe_a") is False


def test_empty_store_get_returns_none(store: ArchiveStore) -> None:
    assert store.get("pipe_a") is None


def test_empty_store_all_returns_empty(store: ArchiveStore) -> None:
    assert store.all() == []


def test_archive_marks_pipeline(store: ArchiveStore) -> None:
    store.archive("pipe_a")
    assert store.is_archived("pipe_a") is True


def test_archive_with_reason_stored(store: ArchiveStore) -> None:
    store.archive("pipe_b", reason="deprecated")
    entry = store.get("pipe_b")
    assert entry is not None
    assert entry.reason == "deprecated"


def test_archive_without_reason_is_none(store: ArchiveStore) -> None:
    store.archive("pipe_c")
    entry = store.get("pipe_c")
    assert entry is not None
    assert entry.reason is None


def test_restore_removes_entry(store: ArchiveStore) -> None:
    store.archive("pipe_a")
    result = store.restore("pipe_a")
    assert result is True
    assert store.is_archived("pipe_a") is False


def test_restore_unknown_returns_false(store: ArchiveStore) -> None:
    result = store.restore("nonexistent")
    assert result is False


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "archive.json"
    s1 = ArchiveStore(path)
    s1.archive("pipe_x", reason="old")

    s2 = ArchiveStore(path)
    assert s2.is_archived("pipe_x") is True
    entry = s2.get("pipe_x")
    assert entry is not None
    assert entry.reason == "old"


def test_all_returns_all_entries(store: ArchiveStore) -> None:
    store.archive("pipe_1")
    store.archive("pipe_2")
    names = {e.pipeline for e in store.all()}
    assert names == {"pipe_1", "pipe_2"}


def test_summary_contains_pipeline_name(store: ArchiveStore) -> None:
    entry = store.archive("pipe_z", reason="test reason")
    summary = entry.summary()
    assert "pipe_z" in summary
    assert "test reason" in summary


def test_summary_without_reason(store: ArchiveStore) -> None:
    entry = store.archive("pipe_q")
    summary = entry.summary()
    assert "pipe_q" in summary
    assert "(" not in summary
