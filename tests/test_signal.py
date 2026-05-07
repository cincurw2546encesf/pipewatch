"""Tests for pipewatch.signal."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.signal import SignalEntry, SignalStore


_FIXED = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def store(tmp_path: Path) -> SignalStore:
    return SignalStore(tmp_path / "signals.json")


def _now():
    return _FIXED


def test_empty_store_returns_no_entries(store):
    assert store.all() == []


def test_get_unknown_pipeline_returns_empty(store):
    assert store.get("pipe_a") == []


def test_emit_creates_entry(store):
    entry = store.emit("pipe_a", "started", "pipeline began", now_fn=_now)
    assert entry.pipeline == "pipe_a"
    assert entry.name == "started"
    assert entry.message == "pipeline began"
    assert entry.emitted_at == _FIXED


def test_emit_persists_across_reload(tmp_path):
    path = tmp_path / "signals.json"
    s1 = SignalStore(path)
    s1.emit("pipe_a", "done", "finished ok", now_fn=_now)
    s2 = SignalStore(path)
    entries = s2.get("pipe_a")
    assert len(entries) == 1
    assert entries[0].name == "done"


def test_get_filters_by_pipeline(store):
    store.emit("pipe_a", "started", "a", now_fn=_now)
    store.emit("pipe_b", "started", "b", now_fn=_now)
    assert len(store.get("pipe_a")) == 1
    assert len(store.get("pipe_b")) == 1


def test_get_by_name_filters_correctly(store):
    store.emit("pipe_a", "started", "msg1", now_fn=_now)
    store.emit("pipe_a", "failed", "msg2", now_fn=_now)
    results = store.get_by_name("pipe_a", "started")
    assert len(results) == 1
    assert results[0].name == "started"


def test_clear_removes_pipeline_entries(store):
    store.emit("pipe_a", "ev", "m", now_fn=_now)
    store.emit("pipe_b", "ev", "m", now_fn=_now)
    removed = store.clear("pipe_a")
    assert removed == 1
    assert store.get("pipe_a") == []
    assert len(store.get("pipe_b")) == 1


def test_clear_returns_zero_for_unknown(store):
    assert store.clear("nonexistent") == 0


def test_summary_format(store):
    entry = store.emit("pipe_a", "check", "all good", now_fn=_now)
    s = entry.summary()
    assert "pipe_a" in s
    assert "check" in s
    assert "all good" in s
    assert "2024-06-01" in s


def test_all_returns_all_entries(store):
    store.emit("pipe_a", "ev1", "m1", now_fn=_now)
    store.emit("pipe_b", "ev2", "m2", now_fn=_now)
    assert len(store.all()) == 2
