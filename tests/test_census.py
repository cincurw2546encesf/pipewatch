"""Tests for pipewatch.census."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytest

from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.checker import CheckStatus
from pipewatch.census import check_census, check_all_census, CensusResult


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(str(history_file))


BASE = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _add(store: HistoryStore, pipeline: str, status: CheckStatus, ago_seconds: int) -> None:
    ts = BASE - timedelta(seconds=ago_seconds)
    entry = HistoryEntry(pipeline=pipeline, status=status, checked_at=ts, last_run=None)
    store.append(entry)


def _now() -> datetime:
    return BASE


def test_no_history_returns_zero_total(store: HistoryStore) -> None:
    result = check_census("pipe", 3600, store, _now)
    assert result.total == 0
    assert result.ok == 0


def test_counts_ok_runs(store: HistoryStore) -> None:
    _add(store, "pipe", CheckStatus.OK, 100)
    _add(store, "pipe", CheckStatus.OK, 200)
    result = check_census("pipe", 3600, store, _now)
    assert result.ok == 2
    assert result.total == 2


def test_counts_mixed_statuses(store: HistoryStore) -> None:
    _add(store, "pipe", CheckStatus.OK, 100)
    _add(store, "pipe", CheckStatus.STALE, 200)
    _add(store, "pipe", CheckStatus.FAILED, 300)
    result = check_census("pipe", 3600, store, _now)
    assert result.ok == 1
    assert result.stale == 1
    assert result.failed == 1
    assert result.total == 3


def test_excludes_entries_outside_window(store: HistoryStore) -> None:
    _add(store, "pipe", CheckStatus.OK, 100)       # inside
    _add(store, "pipe", CheckStatus.FAILED, 7200)  # outside (2 h)
    result = check_census("pipe", 3600, store, _now)
    assert result.total == 1
    assert result.ok == 1
    assert result.failed == 0


def test_summary_no_runs(store: HistoryStore) -> None:
    result = check_census("pipe", 3600, store, _now)
    assert "no runs" in result.summary


def test_summary_with_runs(store: HistoryStore) -> None:
    _add(store, "pipe", CheckStatus.OK, 100)
    result = check_census("pipe", 3600, store, _now)
    assert "ok=1" in result.summary


def test_check_all_census_multiple_pipelines(store: HistoryStore) -> None:
    _add(store, "alpha", CheckStatus.OK, 100)
    _add(store, "beta", CheckStatus.FAILED, 200)
    results = check_all_census(["alpha", "beta"], 3600, store, _now)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"alpha", "beta"}


def test_unknown_status_counted(store: HistoryStore) -> None:
    # Manually inject an entry with an unusual status string
    ts = BASE - timedelta(seconds=100)
    entry = HistoryEntry(pipeline="pipe", status="weird", checked_at=ts, last_run=None)  # type: ignore[arg-type]
    store.append(entry)
    result = check_census("pipe", 3600, store, _now)
    assert result.unknown == 1
    assert result.total == 1
