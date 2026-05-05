"""Tests for pipewatch.streak."""
import json
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.streak import (
    StreakEntry,
    StreakStore,
    check_all_streaks,
    check_streak,
)


@pytest.fixture()
def store(tmp_path: Path) -> StreakStore:
    return StreakStore(tmp_path / "streaks.json")


def _result(pipeline: str, status: CheckStatus) -> CheckResult:
    return CheckResult(pipeline=pipeline, status=status, message="")


# ---------------------------------------------------------------------------
# StreakStore
# ---------------------------------------------------------------------------

def test_empty_store_returns_none(store: StreakStore) -> None:
    assert store.get("pipe_a") is None


def test_record_ok_creates_entry(store: StreakStore) -> None:
    entry = store.record("pipe_a", "ok")
    assert entry.current_status == "ok"
    assert entry.count == 1


def test_same_status_increments_count(store: StreakStore) -> None:
    store.record("pipe_a", "ok")
    store.record("pipe_a", "ok")
    entry = store.get("pipe_a")
    assert entry is not None
    assert entry.count == 2


def test_status_change_resets_count(store: StreakStore) -> None:
    store.record("pipe_a", "ok")
    store.record("pipe_a", "ok")
    entry = store.record("pipe_a", "fail")
    assert entry.current_status == "fail"
    assert entry.count == 1


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "streaks.json"
    s1 = StreakStore(path)
    s1.record("pipe_a", "fail")
    s1.record("pipe_a", "fail")

    s2 = StreakStore(path)
    entry = s2.get("pipe_a")
    assert entry is not None
    assert entry.count == 2
    assert entry.current_status == "fail"


def test_all_returns_all_entries(store: StreakStore) -> None:
    store.record("pipe_a", "ok")
    store.record("pipe_b", "fail")
    assert len(store.all()) == 2


# ---------------------------------------------------------------------------
# check_streak
# ---------------------------------------------------------------------------

def test_check_streak_ok_not_concerning(store: StreakStore) -> None:
    r = _result("pipe_a", CheckStatus.OK)
    sr = check_streak(r, store, fail_threshold=3)
    assert sr.current_status == "ok"
    assert not sr.is_concerning


def test_check_streak_fail_below_threshold(store: StreakStore) -> None:
    r = _result("pipe_a", CheckStatus.STALE)
    store.record("pipe_a", "fail")
    sr = check_streak(r, store, fail_threshold=3)
    assert sr.count == 2
    assert not sr.is_concerning


def test_check_streak_fail_at_threshold(store: StreakStore) -> None:
    r = _result("pipe_a", CheckStatus.FAILED)
    store.record("pipe_a", "fail")
    store.record("pipe_a", "fail")
    sr = check_streak(r, store, fail_threshold=3)
    assert sr.count == 3
    assert sr.is_concerning


def test_check_all_streaks_returns_one_per_result(store: StreakStore) -> None:
    results = [
        _result("pipe_a", CheckStatus.OK),
        _result("pipe_b", CheckStatus.STALE),
    ]
    streak_results = check_all_streaks(results, store)
    assert len(streak_results) == 2
    names = {sr.pipeline for sr in streak_results}
    assert names == {"pipe_a", "pipe_b"}


def test_streak_result_summary_contains_pipeline(store: StreakStore) -> None:
    r = _result("pipe_a", CheckStatus.OK)
    sr = check_streak(r, store)
    assert "pipe_a" in sr.summary()
    assert "ok" in sr.summary()
