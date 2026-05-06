"""Tests for pipewatch.decay."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.decay import (
    DecayStore,
    DecayEntry,
    _compute_score,
    check_decay,
    check_all_decay,
)


@pytest.fixture()
def store(tmp_path: Path) -> DecayStore:
    return DecayStore(tmp_path / "decay.json")


def _dt(days_ago: float = 0) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)


def _now_fn(dt: datetime):
    return lambda: dt


# ---------------------------------------------------------------------------
# DecayEntry / DecayStore
# ---------------------------------------------------------------------------

def test_empty_store_returns_default(store):
    entry = store.get("pipe-a")
    assert entry.failure_count == 0
    assert entry.last_failure is None


def test_record_failure_increments(store):
    store.record_failure("pipe-a")
    store.record_failure("pipe-a")
    assert store.get("pipe-a").failure_count == 2


def test_record_failure_persists(tmp_path):
    p = tmp_path / "decay.json"
    s1 = DecayStore(p)
    s1.record_failure("pipe-x")
    s2 = DecayStore(p)
    assert s2.get("pipe-x").failure_count == 1


def test_reset_removes_entry(store):
    store.record_failure("pipe-b")
    store.reset("pipe-b")
    assert store.get("pipe-b").failure_count == 0


def test_all_entries_returns_dict(store):
    store.record_failure("a")
    store.record_failure("b")
    entries = store.all_entries()
    assert set(entries.keys()) == {"a", "b"}


# ---------------------------------------------------------------------------
# _compute_score
# ---------------------------------------------------------------------------

def test_score_zero_when_no_failures():
    entry = DecayEntry(failure_count=0)
    assert _compute_score(entry) == 0.0


def test_score_positive_after_recent_failure():
    now = datetime.now(timezone.utc)
    entry = DecayEntry(failure_count=5, last_failure=now.isoformat())
    score = _compute_score(entry, now_fn=lambda: now)
    assert 0.0 < score <= 1.0


def test_score_decays_over_time():
    recent = datetime.now(timezone.utc)
    old = recent - timedelta(days=30)
    entry_recent = DecayEntry(failure_count=5, last_failure=recent.isoformat())
    entry_old = DecayEntry(failure_count=5, last_failure=old.isoformat())
    s_recent = _compute_score(entry_recent, now_fn=lambda: recent)
    s_old = _compute_score(entry_old, now_fn=lambda: recent)
    assert s_recent > s_old


# ---------------------------------------------------------------------------
# check_decay
# ---------------------------------------------------------------------------

class _Pipeline:
    def __init__(self, name, decay=None):
        self.name = name
        self.decay = decay


def test_check_decay_no_config_returns_none(store):
    p = _Pipeline("p", decay=None)
    assert check_decay(p, store) is None


def test_check_decay_not_exceeded_below_threshold(store):
    now = datetime.now(timezone.utc)
    store.record_failure("p", now_fn=lambda: now)
    p = _Pipeline("p", decay={"threshold": 0.99, "half_life_days": 7})
    result = check_decay(p, store, now_fn=lambda: now)
    assert result is not None
    assert not result.exceeded


def test_check_decay_exceeded_above_threshold(store):
    now = datetime.now(timezone.utc)
    for _ in range(20):
        store.record_failure("p", now_fn=lambda: now)
    p = _Pipeline("p", decay={"threshold": 0.01, "half_life_days": 7})
    result = check_decay(p, store, now_fn=lambda: now)
    assert result is not None
    assert result.exceeded


def test_check_all_decay_skips_unconfigured(store):
    pipelines = [_Pipeline("a", decay=None), _Pipeline("b", decay={"threshold": 0.5})]
    results = check_all_decay(pipelines, store)
    assert len(results) == 1
    assert results[0].pipeline == "b"


def test_summary_contains_pipeline_name(store):
    now = datetime.now(timezone.utc)
    store.record_failure("my-pipe", now_fn=lambda: now)
    p = _Pipeline("my-pipe", decay={"threshold": 0.5})
    result = check_decay(p, store, now_fn=lambda: now)
    assert "my-pipe" in result.summary()
