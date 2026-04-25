"""Tests for pipewatch.window."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.window import WindowResult, check_all_windows, check_window


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _now_fn():
    return NOW


@pytest.fixture()
def store(tmp_path):
    return HistoryStore(str(tmp_path / "history.json"))


def _add(store: HistoryStore, pipeline: str, status: str, hours_ago: float) -> None:
    ts = NOW - timedelta(hours=hours_ago)
    entry = HistoryEntry(
        pipeline=pipeline,
        status=status,
        checked_at=ts,
        last_run=None,
    )
    store.append(entry)


# ---------------------------------------------------------------------------


def test_check_window_no_history_returns_none(store):
    result = check_window("pipe_a", store, now_fn=_now_fn)
    assert result is None


def test_check_window_all_ok(store):
    for h in [1, 2, 3]:
        _add(store, "pipe_a", "ok", h)
    r = check_window("pipe_a", store, window_hours=24, threshold=0.8, now_fn=_now_fn)
    assert r is not None
    assert r.total == 3
    assert r.ok == 3
    assert r.failed == 0
    assert r.success_rate == pytest.approx(1.0)
    assert not r.violated


def test_check_window_partial_failures(store):
    _add(store, "pipe_b", "ok", 1)
    _add(store, "pipe_b", "ok", 2)
    _add(store, "pipe_b", "stale", 3)
    _add(store, "pipe_b", "stale", 4)
    r = check_window("pipe_b", store, window_hours=24, threshold=0.8, now_fn=_now_fn)
    assert r.total == 4
    assert r.ok == 2
    assert r.success_rate == pytest.approx(0.5)
    assert r.violated  # 0.5 < 0.8


def test_check_window_excludes_old_entries(store):
    _add(store, "pipe_c", "ok", 1)   # inside window
    _add(store, "pipe_c", "stale", 30)  # outside 24-h window
    r = check_window("pipe_c", store, window_hours=24, threshold=0.8, now_fn=_now_fn)
    assert r.total == 1
    assert r.ok == 1
    assert not r.violated


def test_check_window_exactly_at_threshold(store):
    _add(store, "pipe_d", "ok", 1)
    _add(store, "pipe_d", "stale", 2)
    r = check_window("pipe_d", store, window_hours=24, threshold=0.5, now_fn=_now_fn)
    assert r.success_rate == pytest.approx(0.5)
    assert not r.violated  # rate >= threshold is OK


def test_check_all_windows_skips_pipelines_without_history(store):
    _add(store, "pipe_e", "ok", 1)
    results = check_all_windows(
        ["pipe_e", "pipe_missing"], store, now_fn=_now_fn
    )
    assert len(results) == 1
    assert results[0].pipeline == "pipe_e"


def test_window_result_summary_ok(store):
    _add(store, "pipe_f", "ok", 1)
    r = check_window("pipe_f", store, window_hours=6, threshold=0.8, now_fn=_now_fn)
    s = r.summary()
    assert "pipe_f" in s
    assert "OK" in s
    assert "6h" in s


def test_window_result_summary_violated(store):
    _add(store, "pipe_g", "stale", 1)
    r = check_window("pipe_g", store, window_hours=24, threshold=0.8, now_fn=_now_fn)
    s = r.summary()
    assert "VIOLATED" in s
