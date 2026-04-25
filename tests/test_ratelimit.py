"""Tests for pipewatch.ratelimit."""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.ratelimit import RateLimitStore


@pytest.fixture
def store(tmp_path: Path) -> RateLimitStore:
    return RateLimitStore(tmp_path / "ratelimit.json")


def test_empty_store_not_rate_limited(store: RateLimitStore) -> None:
    assert store.is_rate_limited("pipe_a", min_interval_seconds=60) is False


def test_record_check_marks_pipeline(store: RateLimitStore) -> None:
    store.record_check("pipe_a")
    assert store.is_rate_limited("pipe_a", min_interval_seconds=60) is True


def test_rate_limit_expires(store: RateLimitStore, tmp_path: Path) -> None:
    past = time.time() - 120  # 2 minutes ago
    with patch("pipewatch.ratelimit._utcnow", return_value=past):
        store.record_check("pipe_a")
    assert store.is_rate_limited("pipe_a", min_interval_seconds=60) is False


def test_reset_removes_entry(store: RateLimitStore) -> None:
    store.record_check("pipe_a")
    store.reset("pipe_a")
    assert store.is_rate_limited("pipe_a", min_interval_seconds=60) is False


def test_check_count_increments(store: RateLimitStore) -> None:
    store.record_check("pipe_a")
    store.record_check("pipe_a")
    store.record_check("pipe_a")
    entry = store.get("pipe_a")
    assert entry is not None
    assert entry.check_count == 3


def test_persists_across_reload(store: RateLimitStore, tmp_path: Path) -> None:
    store.record_check("pipe_b")
    reloaded = RateLimitStore(tmp_path / "ratelimit.json")
    assert reloaded.is_rate_limited("pipe_b", min_interval_seconds=60) is True


def test_get_unknown_returns_none(store: RateLimitStore) -> None:
    assert store.get("nonexistent") is None


def test_all_entries_returns_all(store: RateLimitStore) -> None:
    store.record_check("pipe_a")
    store.record_check("pipe_b")
    names = {e.pipeline for e in store.all_entries()}
    assert names == {"pipe_a", "pipe_b"}


def test_reset_nonexistent_is_safe(store: RateLimitStore) -> None:
    store.reset("ghost_pipeline")  # should not raise


def test_independent_pipelines(store: RateLimitStore, tmp_path: Path) -> None:
    past = time.time() - 120
    with patch("pipewatch.ratelimit._utcnow", return_value=past):
        store.record_check("pipe_old")
    store.record_check("pipe_new")
    assert store.is_rate_limited("pipe_old", min_interval_seconds=60) is False
    assert store.is_rate_limited("pipe_new", min_interval_seconds=60) is True
