"""Tests for pipewatch.suppression."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.suppression import SuppressionEntry, SuppressionStore

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def store(tmp_path: Path) -> SuppressionStore:
    return SuppressionStore(tmp_path / "suppressions.json")


def test_empty_store_not_suppressed(store: SuppressionStore) -> None:
    assert not store.is_suppressed("pipeline_a", now=_NOW)


def test_suppress_pipeline(store: SuppressionStore) -> None:
    until = _NOW + timedelta(hours=1)
    store.suppress("pipeline_a", reason="maintenance", until=until)
    assert store.is_suppressed("pipeline_a", now=_NOW)


def test_suppression_expired(store: SuppressionStore) -> None:
    until = _NOW - timedelta(seconds=1)
    store.suppress("pipeline_a", reason="old", until=until)
    assert not store.is_suppressed("pipeline_a", now=_NOW)


def test_remove_suppression(store: SuppressionStore) -> None:
    until = _NOW + timedelta(hours=2)
    store.suppress("pipeline_a", reason="", until=until)
    removed = store.remove("pipeline_a")
    assert removed is True
    assert not store.is_suppressed("pipeline_a", now=_NOW)


def test_remove_nonexistent_returns_false(store: SuppressionStore) -> None:
    assert store.remove("no_such_pipeline") is False


def test_all_active_filters_expired(store: SuppressionStore) -> None:
    store.suppress("active_pipe", reason="", until=_NOW + timedelta(hours=1))
    store.suppress("expired_pipe", reason="", until=_NOW - timedelta(seconds=1))
    active = store.all_active(now=_NOW)
    names = [e.pipeline for e in active]
    assert "active_pipe" in names
    assert "expired_pipe" not in names


def test_purge_expired_removes_old_entries(store: SuppressionStore, tmp_path: Path) -> None:
    store.suppress("pipe_a", reason="", until=_NOW + timedelta(hours=1))
    store.suppress("pipe_b", reason="", until=_NOW - timedelta(seconds=1))
    count = store.purge_expired(now=_NOW)
    assert count == 1
    assert store.is_suppressed("pipe_a", now=_NOW)
    assert not store.is_suppressed("pipe_b", now=_NOW)


def test_persists_across_reload(store: SuppressionStore, tmp_path: Path) -> None:
    until = _NOW + timedelta(hours=3)
    store.suppress("pipe_x", reason="deploy", until=until)
    reloaded = SuppressionStore(tmp_path / "suppressions.json")
    assert reloaded.is_suppressed("pipe_x", now=_NOW)
    entry = reloaded.get("pipe_x")
    assert entry is not None
    assert entry.reason == "deploy"


def test_get_returns_none_for_unknown(store: SuppressionStore) -> None:
    assert store.get("unknown") is None


def test_entry_is_active_boundary(store: SuppressionStore) -> None:
    until = _NOW
    entry = SuppressionEntry(pipeline="p", reason="", suppressed_until=until)
    # exactly at boundary: now == suppressed_until means NOT active (strict <)
    assert not entry.is_active(now=_NOW)
