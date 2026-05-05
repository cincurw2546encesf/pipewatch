"""Tests for pipewatch.pause."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.pause import PauseStore


@pytest.fixture()
def store(tmp_path: Path) -> PauseStore:
    return PauseStore(tmp_path / "pauses.json")


def test_empty_store_not_paused(store: PauseStore) -> None:
    assert store.is_paused("my_pipeline") is False


def test_pause_pipeline(store: PauseStore) -> None:
    entry = store.pause("etl_daily")
    assert entry.pipeline == "etl_daily"
    assert store.is_paused("etl_daily") is True


def test_pause_with_reason(store: PauseStore) -> None:
    store.pause("etl_daily", reason="maintenance window")
    entries = store.all_paused()
    assert len(entries) == 1
    assert entries[0].reason == "maintenance window"


def test_resume_removes_entry(store: PauseStore) -> None:
    store.pause("etl_daily")
    result = store.resume("etl_daily")
    assert result is True
    assert store.is_paused("etl_daily") is False


def test_resume_unknown_returns_false(store: PauseStore) -> None:
    result = store.resume("nonexistent")
    assert result is False


def test_all_paused_returns_all(store: PauseStore) -> None:
    store.pause("pipe_a")
    store.pause("pipe_b", reason="testing")
    names = {e.pipeline for e in store.all_paused()}
    assert names == {"pipe_a", "pipe_b"}


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "pauses.json"
    s1 = PauseStore(path)
    s1.pause("etl_daily", reason="deploy")

    s2 = PauseStore(path)
    assert s2.is_paused("etl_daily") is True
    assert s2.all_paused()[0].reason == "deploy"


def test_resume_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "pauses.json"
    s1 = PauseStore(path)
    s1.pause("etl_daily")
    s1.resume("etl_daily")

    s2 = PauseStore(path)
    assert s2.is_paused("etl_daily") is False
