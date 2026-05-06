"""Tests for pipewatch.fence."""
from __future__ import annotations

import json
import os

import pytest

from pipewatch.fence import FenceStore


@pytest.fixture()
def store(tmp_path):
    return FenceStore(str(tmp_path / "fence.json"))


def test_empty_store_not_locked(store):
    result = store.check("my_pipeline")
    assert result.locked is False
    assert result.owner is None


def test_acquire_succeeds_when_unlocked(store):
    acquired = store.acquire("my_pipeline", "host-1")
    assert acquired is True


def test_acquire_sets_owner(store):
    store.acquire("my_pipeline", "host-1")
    result = store.check("my_pipeline")
    assert result.locked is True
    assert result.owner == "host-1"
    assert result.locked_at is not None


def test_acquire_fails_when_already_locked(store):
    store.acquire("my_pipeline", "host-1")
    acquired = store.acquire("my_pipeline", "host-2")
    assert acquired is False


def test_second_acquire_does_not_overwrite_owner(store):
    store.acquire("my_pipeline", "host-1")
    store.acquire("my_pipeline", "host-2")
    result = store.check("my_pipeline")
    assert result.owner == "host-1"


def test_release_removes_fence(store):
    store.acquire("my_pipeline", "host-1")
    released = store.release("my_pipeline")
    assert released is True
    result = store.check("my_pipeline")
    assert result.locked is False


def test_release_returns_false_when_not_locked(store):
    released = store.release("my_pipeline")
    assert released is False


def test_all_entries_empty(store):
    assert store.all_entries() == []


def test_all_entries_shows_active_fences(store):
    store.acquire("pipe_a", "host-1")
    store.acquire("pipe_b", "host-2")
    entries = store.all_entries()
    names = {e.pipeline for e in entries}
    assert names == {"pipe_a", "pipe_b"}


def test_persists_across_reload(tmp_path):
    path = str(tmp_path / "fence.json")
    s1 = FenceStore(path)
    s1.acquire("my_pipeline", "host-1")

    s2 = FenceStore(path)
    result = s2.check("my_pipeline")
    assert result.locked is True
    assert result.owner == "host-1"


def test_summary_locked(store):
    store.acquire("my_pipeline", "host-1")
    result = store.check("my_pipeline")
    assert "LOCKED" in result.summary()
    assert "host-1" in result.summary()


def test_summary_unlocked(store):
    result = store.check("my_pipeline")
    assert "no active fence" in result.summary()
