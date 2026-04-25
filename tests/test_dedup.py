"""Tests for pipewatch.dedup."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.dedup import DedupStore


@pytest.fixture()
def store(tmp_path):
    return DedupStore(str(tmp_path / "dedup.json"), cooldown_minutes=60)


def _dt(minutes_ago: int = 0) -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


# ---------------------------------------------------------------------------
# is_duplicate
# ---------------------------------------------------------------------------

def test_not_duplicate_when_no_entry(store):
    assert store.is_duplicate("pipe_a", "STALE") is False


def test_duplicate_within_cooldown(store):
    now = _dt(0)
    store.record("pipe_a", "STALE", now=now)
    assert store.is_duplicate("pipe_a", "STALE", now=now + timedelta(minutes=30)) is True


def test_not_duplicate_after_cooldown(store):
    past = _dt(90)
    store.record("pipe_a", "STALE", now=past)
    assert store.is_duplicate("pipe_a", "STALE", now=_dt(0)) is False


def test_different_status_not_duplicate(store):
    now = _dt(0)
    store.record("pipe_a", "STALE", now=now)
    assert store.is_duplicate("pipe_a", "FAILED", now=now) is False


def test_different_pipeline_not_duplicate(store):
    now = _dt(0)
    store.record("pipe_a", "STALE", now=now)
    assert store.is_duplicate("pipe_b", "STALE", now=now) is False


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------

def test_record_creates_entry(store):
    entry = store.record("pipe_a", "FAILED")
    assert entry.pipeline == "pipe_a"
    assert entry.status == "FAILED"
    assert entry.count == 1


def test_record_increments_count(store):
    t1 = _dt(30)
    t2 = _dt(0)
    store.record("pipe_a", "STALE", now=t1)
    entry = store.record("pipe_a", "STALE", now=t2)
    assert entry.count == 2
    assert entry.first_seen == t1
    assert entry.last_seen == t2


def test_record_persists_to_disk(tmp_path):
    path = str(tmp_path / "dedup.json")
    s1 = DedupStore(path, cooldown_minutes=60)
    s1.record("pipe_x", "STALE")

    s2 = DedupStore(path, cooldown_minutes=60)
    entries = s2.all_entries()
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_x"


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def test_reset_removes_entry(store):
    now = _dt(0)
    store.record("pipe_a", "STALE", now=now)
    store.reset("pipe_a", "STALE")
    assert store.is_duplicate("pipe_a", "STALE", now=now) is False


def test_reset_unknown_does_not_raise(store):
    store.reset("nonexistent", "STALE")  # should not raise


# ---------------------------------------------------------------------------
# all_entries
# ---------------------------------------------------------------------------

def test_all_entries_empty(store):
    assert store.all_entries() == []


def test_all_entries_returns_all(store):
    store.record("pipe_a", "STALE")
    store.record("pipe_b", "FAILED")
    assert len(store.all_entries()) == 2
