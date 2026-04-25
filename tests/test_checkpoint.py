"""Tests for pipewatch.checkpoint."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checkpoint import CheckpointEntry, CheckpointStore


@pytest.fixture
def store(tmp_path: Path) -> CheckpointStore:
    return CheckpointStore(tmp_path / "checkpoints.json")


def test_empty_store_returns_no_entries(store: CheckpointStore) -> None:
    assert store.all_entries() == []


def test_empty_store_latest_returns_none(store: CheckpointStore) -> None:
    assert store.latest("pipe_a") is None


def test_record_creates_entry(store: CheckpointStore) -> None:
    entry = store.record("pipe_a", "extract_done")
    assert entry.pipeline == "pipe_a"
    assert entry.name == "extract_done"
    assert isinstance(entry.recorded_at, datetime)


def test_record_with_metadata(store: CheckpointStore) -> None:
    entry = store.record("pipe_b", "load_done", metadata={"rows": "1000"})
    assert entry.metadata["rows"] == "1000"


def test_get_returns_only_matching(store: CheckpointStore) -> None:
    store.record("pipe_a", "step1")
    store.record("pipe_b", "step1")
    store.record("pipe_a", "step2")
    results = store.get("pipe_a")
    assert len(results) == 2
    assert all(e.pipeline == "pipe_a" for e in results)


def test_latest_returns_last_recorded(store: CheckpointStore) -> None:
    store.record("pipe_a", "step1")
    store.record("pipe_a", "step2")
    latest = store.latest("pipe_a")
    assert latest is not None
    assert latest.name == "step2"


def test_clear_removes_entries(store: CheckpointStore) -> None:
    store.record("pipe_a", "step1")
    store.record("pipe_a", "step2")
    store.record("pipe_b", "step1")
    removed = store.clear("pipe_a")
    assert removed == 2
    assert store.get("pipe_a") == []
    assert len(store.get("pipe_b")) == 1


def test_clear_unknown_pipeline_returns_zero(store: CheckpointStore) -> None:
    removed = store.clear("nonexistent")
    assert removed == 0


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "checkpoints.json"
    s1 = CheckpointStore(path)
    s1.record("pipe_a", "done", metadata={"source": "s3"})
    s2 = CheckpointStore(path)
    entries = s2.get("pipe_a")
    assert len(entries) == 1
    assert entries[0].name == "done"
    assert entries[0].metadata["source"] == "s3"


def test_entry_summary_format() -> None:
    entry = CheckpointEntry(
        pipeline="my_pipe",
        name="transform",
        recorded_at=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    summary = entry.summary()
    assert "my_pipe" in summary
    assert "transform" in summary
    assert "2024-06-01" in summary


def test_to_dict_and_from_dict_roundtrip() -> None:
    entry = CheckpointEntry(
        pipeline="pipe_x",
        name="checkpoint_1",
        recorded_at=datetime(2024, 1, 15, 8, 30, 0, tzinfo=timezone.utc),
        metadata={"env": "prod"},
    )
    restored = CheckpointEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.name == entry.name
    assert restored.recorded_at == entry.recorded_at
    assert restored.metadata == entry.metadata
