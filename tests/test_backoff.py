"""Tests for pipewatch.backoff."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.backoff import (
    BackoffEntry,
    BackoffStore,
    cooldown_seconds,
    should_alert,
)


@pytest.fixture()
def store(tmp_path: Path) -> BackoffStore:
    return BackoffStore(tmp_path / "backoff.json")


def _dt(offset_seconds: float = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


# ---------------------------------------------------------------------------
# cooldown_seconds
# ---------------------------------------------------------------------------

def test_cooldown_first_failure_equals_base():
    entry = BackoffEntry(pipeline="p", consecutive_failures=1)
    assert cooldown_seconds(entry, base=60) == 60


def test_cooldown_doubles_each_step():
    assert cooldown_seconds(BackoffEntry("p", 2), base=60) == 120
    assert cooldown_seconds(BackoffEntry("p", 3), base=60) == 240
    assert cooldown_seconds(BackoffEntry("p", 4), base=60) == 480


def test_cooldown_capped_at_max():
    entry = BackoffEntry(pipeline="p", consecutive_failures=20)
    assert cooldown_seconds(entry, base=60, max_seconds=3600) == 3600


def test_cooldown_zero_failures_returns_base():
    entry = BackoffEntry(pipeline="p", consecutive_failures=0)
    assert cooldown_seconds(entry, base=60) == 60


# ---------------------------------------------------------------------------
# should_alert
# ---------------------------------------------------------------------------

def test_should_alert_no_previous_alert():
    entry = BackoffEntry(pipeline="p", consecutive_failures=1, last_alerted_at=None)
    assert should_alert(entry) is True


def test_should_alert_within_cooldown_returns_false():
    alerted = _dt(0)
    entry = BackoffEntry(pipeline="p", consecutive_failures=1, last_alerted_at=alerted)
    now_fn = lambda: _dt(30)  # only 30 s later, cooldown is 60 s
    assert should_alert(entry, base=60, now_fn=now_fn) is False


def test_should_alert_after_cooldown_returns_true():
    alerted = _dt(0)
    entry = BackoffEntry(pipeline="p", consecutive_failures=1, last_alerted_at=alerted)
    now_fn = lambda: _dt(61)
    assert should_alert(entry, base=60, now_fn=now_fn) is True


def test_should_alert_exactly_at_boundary_returns_true():
    alerted = _dt(0)
    entry = BackoffEntry(pipeline="p", consecutive_failures=1, last_alerted_at=alerted)
    now_fn = lambda: _dt(60)
    assert should_alert(entry, base=60, now_fn=now_fn) is True


# ---------------------------------------------------------------------------
# BackoffStore
# ---------------------------------------------------------------------------

def test_empty_store_returns_zero_failures(store: BackoffStore):
    entry = store.get("my_pipeline")
    assert entry.consecutive_failures == 0
    assert entry.last_alerted_at is None


def test_record_failure_increments(store: BackoffStore):
    store.record_failure("p", now_fn=lambda: _dt(0))
    store.record_failure("p", now_fn=lambda: _dt(60))
    entry = store.get("p")
    assert entry.consecutive_failures == 2


def test_record_failure_updates_last_alerted(store: BackoffStore):
    t = _dt(100)
    store.record_failure("p", now_fn=lambda: t)
    assert store.get("p").last_alerted_at == t


def test_reset_removes_entry(store: BackoffStore):
    store.record_failure("p", now_fn=lambda: _dt(0))
    store.reset("p")
    assert store.get("p").consecutive_failures == 0


def test_persists_across_reload(tmp_path: Path):
    path = tmp_path / "backoff.json"
    s1 = BackoffStore(path)
    s1.record_failure("pipe", now_fn=lambda: _dt(0))
    s1.record_failure("pipe", now_fn=lambda: _dt(60))

    s2 = BackoffStore(path)
    assert s2.get("pipe").consecutive_failures == 2


def test_independent_pipelines_tracked_separately(store: BackoffStore):
    store.record_failure("a", now_fn=lambda: _dt(0))
    store.record_failure("a", now_fn=lambda: _dt(0))
    store.record_failure("b", now_fn=lambda: _dt(0))
    assert store.get("a").consecutive_failures == 2
    assert store.get("b").consecutive_failures == 1
