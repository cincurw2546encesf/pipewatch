"""Tests for pipewatch.silencer."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import pytest

from pipewatch.silencer import SilenceEntry, SilenceStore

FUTURE = datetime.now(timezone.utc) + timedelta(hours=2)
PAST = datetime.now(timezone.utc) - timedelta(hours=1)


@pytest.fixture
def store(tmp_path: Path) -> SilenceStore:
    return SilenceStore(tmp_path / "silences.json")


def test_empty_store_not_silenced(store):
    assert not store.is_silenced("pipe_a")


def test_silence_pipeline(store):
    store.silence("pipe_a", FUTURE, reason="maintenance")
    assert store.is_silenced("pipe_a")


def test_silence_expired(store):
    store.silence("pipe_a", PAST)
    assert not store.is_silenced("pipe_a")


def test_unsilence_removes_entry(store):
    store.silence("pipe_a", FUTURE)
    removed = store.unsilence("pipe_a")
    assert removed is True
    assert not store.is_silenced("pipe_a")


def test_unsilence_missing_returns_false(store):
    assert store.unsilence("nonexistent") is False


def test_active_entries_filters_expired(store):
    store.silence("pipe_a", FUTURE)
    store.silence("pipe_b", PAST)
    active = store.active_entries()
    assert len(active) == 1
    assert active[0].pipeline == "pipe_a"


def test_prune_removes_expired(store):
    store.silence("pipe_a", FUTURE)
    store.silence("pipe_b", PAST)
    pruned = store.prune()
    assert pruned == 1
    assert store.is_silenced("pipe_a")


def test_persists_across_reload(tmp_path):
    path = tmp_path / "silences.json"
    s1 = SilenceStore(path)
    s1.silence("pipe_x", FUTURE, reason="test")
    s2 = SilenceStore(path)
    assert s2.is_silenced("pipe_x")
    assert s2.active_entries()[0].reason == "test"


def test_silence_overwrites_existing(store):
    store.silence("pipe_a", PAST)
    store.silence("pipe_a", FUTURE)
    assert store.is_silenced("pipe_a")
    assert len(store.active_entries()) == 1


def test_entry_is_active_with_custom_now():
    entry = SilenceEntry(pipeline="p", until=FUTURE.isoformat())
    assert entry.is_active(datetime.now(timezone.utc))
    assert not entry.is_active(FUTURE + timedelta(seconds=1))
