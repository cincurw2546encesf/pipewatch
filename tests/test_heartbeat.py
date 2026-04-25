"""Tests for pipewatch.heartbeat."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.heartbeat import HeartbeatEntry, HeartbeatStore


@pytest.fixture()
def store(tmp_path: Path) -> HeartbeatStore:
    return HeartbeatStore(tmp_path / "heartbeats.json")


def _dt(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


# ── HeartbeatEntry ────────────────────────────────────────────────────────────

def test_entry_alive_within_interval() -> None:
    entry = HeartbeatEntry(pipeline="p", last_beat=_dt(0), interval_seconds=60)
    assert entry.is_alive(now=_dt(30)) is True


def test_entry_alive_exactly_at_boundary() -> None:
    entry = HeartbeatEntry(pipeline="p", last_beat=_dt(0), interval_seconds=60)
    assert entry.is_alive(now=_dt(60)) is True


def test_entry_dead_past_interval() -> None:
    entry = HeartbeatEntry(pipeline="p", last_beat=_dt(0), interval_seconds=60)
    assert entry.is_alive(now=_dt(61)) is False


def test_entry_seconds_since() -> None:
    entry = HeartbeatEntry(pipeline="p", last_beat=_dt(0), interval_seconds=60)
    assert entry.seconds_since(now=_dt(45)) == pytest.approx(45.0)


def test_entry_roundtrip() -> None:
    entry = HeartbeatEntry(pipeline="etl", last_beat=_dt(0), interval_seconds=120)
    restored = HeartbeatEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.last_beat == entry.last_beat
    assert restored.interval_seconds == entry.interval_seconds


# ── HeartbeatStore ────────────────────────────────────────────────────────────

def test_empty_store_returns_none(store: HeartbeatStore) -> None:
    assert store.get("missing") is None


def test_beat_records_entry(store: HeartbeatStore) -> None:
    store.beat("pipe_a", interval_seconds=300, now=_dt(0))
    entry = store.get("pipe_a")
    assert entry is not None
    assert entry.interval_seconds == 300


def test_beat_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "hb.json"
    s1 = HeartbeatStore(path)
    s1.beat("pipe_b", interval_seconds=60, now=_dt(0))
    s2 = HeartbeatStore(path)
    assert s2.get("pipe_b") is not None


def test_check_alive(store: HeartbeatStore) -> None:
    store.beat("pipe_c", interval_seconds=300, now=_dt(0))
    report = store.check("pipe_c", now=_dt(100))
    assert report is not None
    assert report.alive is True


def test_check_missed(store: HeartbeatStore) -> None:
    store.beat("pipe_d", interval_seconds=60, now=_dt(0))
    report = store.check("pipe_d", now=_dt(120))
    assert report is not None
    assert report.alive is False


def test_check_unknown_returns_none(store: HeartbeatStore) -> None:
    assert store.check("ghost") is None


def test_check_all_returns_all(store: HeartbeatStore) -> None:
    store.beat("p1", 60, now=_dt(0))
    store.beat("p2", 60, now=_dt(0))
    reports = store.check_all(now=_dt(10))
    names = {r.pipeline for r in reports}
    assert names == {"p1", "p2"}


def test_remove_entry(store: HeartbeatStore) -> None:
    store.beat("pipe_e", 60, now=_dt(0))
    removed = store.remove("pipe_e")
    assert removed is True
    assert store.get("pipe_e") is None


def test_remove_nonexistent_returns_false(store: HeartbeatStore) -> None:
    assert store.remove("nobody") is False


def test_report_str_alive(store: HeartbeatStore) -> None:
    store.beat("pipe_f", 300, now=_dt(0))
    report = store.check("pipe_f", now=_dt(10))
    assert "ALIVE" in str(report)
    assert "pipe_f" in str(report)


def test_report_str_missed(store: HeartbeatStore) -> None:
    store.beat("pipe_g", 60, now=_dt(0))
    report = store.check("pipe_g", now=_dt(200))
    assert "MISSED" in str(report)
