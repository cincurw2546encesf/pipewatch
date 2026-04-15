"""Tests for pipewatch.history module."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.history import HistoryEntry, HistoryStore


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(path=history_file, max_entries=10)


@pytest.fixture()
def stale_result() -> CheckResult:
    return CheckResult(
        pipeline="pipe_a",
        status=CheckStatus.STALE,
        last_run=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        message="stale",
    )


@pytest.fixture()
def ok_result() -> CheckResult:
    return CheckResult(
        pipeline="pipe_b",
        status=CheckStatus.OK,
        last_run=datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc),
        message="ok",
    )


def test_empty_store_returns_no_entries(store: HistoryStore) -> None:
    assert store.all_entries() == []


def test_record_creates_file(store: HistoryStore, stale_result: CheckResult) -> None:
    store.record(stale_result)
    assert store.path.exists()


def test_record_persists_entry(store: HistoryStore, stale_result: CheckResult) -> None:
    store.record(stale_result)
    entries = store.all_entries()
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_a"
    assert entries[0].status == "stale"


def test_for_pipeline_filters_correctly(
    store: HistoryStore, stale_result: CheckResult, ok_result: CheckResult
) -> None:
    store.record(stale_result)
    store.record(ok_result)
    pipe_a = store.for_pipeline("pipe_a")
    assert len(pipe_a) == 1
    assert pipe_a[0].pipeline == "pipe_a"


def test_max_entries_enforced(history_file: Path, stale_result: CheckResult) -> None:
    store = HistoryStore(path=history_file, max_entries=3)
    for _ in range(5):
        store.record(stale_result)
    assert len(store.all_entries()) == 3


def test_clear_all(store: HistoryStore, stale_result: CheckResult, ok_result: CheckResult) -> None:
    store.record(stale_result)
    store.record(ok_result)
    removed = store.clear()
    assert removed == 2
    assert store.all_entries() == []


def test_clear_specific_pipeline(
    store: HistoryStore, stale_result: CheckResult, ok_result: CheckResult
) -> None:
    store.record(stale_result)
    store.record(ok_result)
    removed = store.clear(pipeline="pipe_a")
    assert removed == 1
    remaining = store.all_entries()
    assert len(remaining) == 1
    assert remaining[0].pipeline == "pipe_b"


def test_missing_file_does_not_raise(store: HistoryStore) -> None:
    entries = store.for_pipeline("nonexistent")
    assert entries == []


def test_entry_from_result_has_last_run(stale_result: CheckResult) -> None:
    entry = HistoryEntry.from_result(stale_result)
    assert entry.last_run == "2024-01-01T10:00:00+00:00"


def test_entry_from_result_none_last_run() -> None:
    result = CheckResult(pipeline="p", status=CheckStatus.MISSING, last_run=None, message="no run")
    entry = HistoryEntry.from_result(result)
    assert entry.last_run is None
