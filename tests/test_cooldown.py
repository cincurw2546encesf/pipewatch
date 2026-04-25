"""Tests for pipewatch.cooldown."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.cooldown import CooldownEntry, CooldownStore


@pytest.fixture()
def store(tmp_path: Path) -> CooldownStore:
    return CooldownStore(tmp_path / "cooldowns.json")


def _dt(offset_seconds: float = 0.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)


# ---------------------------------------------------------------------------
# CooldownEntry unit tests
# ---------------------------------------------------------------------------

def test_entry_is_cooling_within_window() -> None:
    entry = CooldownEntry(
        pipeline="p1",
        alerted_at=_dt(-30),
        cooldown_seconds=120,
    )
    assert entry.is_cooling() is True


def test_entry_not_cooling_after_window() -> None:
    entry = CooldownEntry(
        pipeline="p1",
        alerted_at=_dt(-200),
        cooldown_seconds=120,
    )
    assert entry.is_cooling() is False


def test_seconds_remaining_positive_within_window() -> None:
    entry = CooldownEntry(
        pipeline="p1",
        alerted_at=_dt(-60),
        cooldown_seconds=120,
    )
    remaining = entry.seconds_remaining()
    assert 50 < remaining <= 60


def test_seconds_remaining_zero_after_window() -> None:
    entry = CooldownEntry(
        pipeline="p1",
        alerted_at=_dt(-300),
        cooldown_seconds=120,
    )
    assert entry.seconds_remaining() == 0.0


def test_round_trip_serialisation() -> None:
    entry = CooldownEntry(
        pipeline="pipe-x",
        alerted_at=_dt(-10),
        cooldown_seconds=300,
    )
    restored = CooldownEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.cooldown_seconds == entry.cooldown_seconds
    assert abs((restored.alerted_at - entry.alerted_at).total_seconds()) < 1


# ---------------------------------------------------------------------------
# CooldownStore tests
# ---------------------------------------------------------------------------

def test_empty_store_not_cooling(store: CooldownStore) -> None:
    assert store.is_cooling("missing") is False


def test_record_alert_marks_cooling(store: CooldownStore) -> None:
    store.record_alert("pipe-a", cooldown_seconds=600)
    assert store.is_cooling("pipe-a") is True


def test_record_alert_expired_not_cooling(store: CooldownStore) -> None:
    entry = store.record_alert("pipe-b", cooldown_seconds=1)
    past = _dt(-100)
    assert entry.is_cooling(now=past) is False


def test_reset_removes_entry(store: CooldownStore) -> None:
    store.record_alert("pipe-c", cooldown_seconds=600)
    assert store.reset("pipe-c") is True
    assert store.is_cooling("pipe-c") is False


def test_reset_missing_returns_false(store: CooldownStore) -> None:
    assert store.reset("nonexistent") is False


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "cooldowns.json"
    s1 = CooldownStore(path)
    s1.record_alert("pipe-d", cooldown_seconds=900)

    s2 = CooldownStore(path)
    assert s2.is_cooling("pipe-d") is True


def test_all_entries_returns_list(store: CooldownStore) -> None:
    store.record_alert("p1", 60)
    store.record_alert("p2", 120)
    names = {e.pipeline for e in store.all_entries()}
    assert names == {"p1", "p2"}
