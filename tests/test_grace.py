"""Tests for pipewatch.grace."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.grace import GraceEntry, GraceStore


def _dt(offset_seconds: float = 0.0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


@pytest.fixture()
def store(tmp_path: Path) -> GraceStore:
    return GraceStore(tmp_path / "grace.json")


def test_empty_store_not_in_grace(store: GraceStore) -> None:
    assert store.is_in_grace("pipe_a") is False


def test_register_creates_entry(store: GraceStore) -> None:
    entry = store.register("pipe_a", 300, now=_dt())
    assert entry.pipeline == "pipe_a"
    assert entry.grace_seconds == 300


def test_entry_active_within_window() -> None:
    entry = GraceEntry(pipeline="p", registered_at=_dt(), grace_seconds=60)
    assert entry.is_active(now=_dt(30)) is True


def test_entry_active_exactly_at_boundary() -> None:
    entry = GraceEntry(pipeline="p", registered_at=_dt(), grace_seconds=60)
    # strictly less than, so at boundary it is inactive
    assert entry.is_active(now=_dt(60)) is False


def test_entry_inactive_after_window() -> None:
    entry = GraceEntry(pipeline="p", registered_at=_dt(), grace_seconds=60)
    assert entry.is_active(now=_dt(120)) is False


def test_seconds_remaining_positive(store: GraceStore) -> None:
    store.register("pipe_b", 120, now=_dt())
    entry = store.get("pipe_b")
    assert entry is not None
    remaining = entry.seconds_remaining(now=_dt(50))
    assert abs(remaining - 70.0) < 0.01


def test_seconds_remaining_zero_after_expiry() -> None:
    entry = GraceEntry(pipeline="p", registered_at=_dt(), grace_seconds=30)
    assert entry.seconds_remaining(now=_dt(200)) == 0.0


def test_is_in_grace_delegates_to_entry(store: GraceStore) -> None:
    store.register("pipe_c", 600, now=_dt())
    assert store.is_in_grace("pipe_c", now=_dt(100)) is True
    assert store.is_in_grace("pipe_c", now=_dt(700)) is False


def test_remove_entry(store: GraceStore) -> None:
    store.register("pipe_d", 60, now=_dt())
    assert store.remove("pipe_d") is True
    assert store.get("pipe_d") is None


def test_remove_missing_returns_false(store: GraceStore) -> None:
    assert store.remove("nonexistent") is False


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "grace.json"
    s1 = GraceStore(path)
    s1.register("pipe_e", 500, now=_dt())
    s2 = GraceStore(path)
    entry = s2.get("pipe_e")
    assert entry is not None
    assert entry.grace_seconds == 500


def test_all_entries_returns_list(store: GraceStore) -> None:
    store.register("pipe_f", 60, now=_dt())
    store.register("pipe_g", 120, now=_dt())
    names = {e.pipeline for e in store.all_entries()}
    assert names == {"pipe_f", "pipe_g"}
