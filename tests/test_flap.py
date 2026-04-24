"""Tests for pipewatch.flap — flap detection logic."""
from __future__ import annotations

import json
import pathlib
from datetime import datetime, timezone

import pytest

from pipewatch.checker import CheckStatus
from pipewatch.flap import (
    FlapResult,
    _count_transitions,
    check_flap,
    check_all_flap,
    DEFAULT_THRESHOLD,
    DEFAULT_WINDOW,
)
from pipewatch.history import HistoryEntry, HistoryStore


_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _entry(pipeline: str, status: str) -> dict:
    return {
        "pipeline": pipeline,
        "status": status,
        "checked_at": _NOW.isoformat(),
        "last_run": None,
    }


@pytest.fixture()
def history_file(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: pathlib.Path) -> HistoryStore:
    return HistoryStore(str(history_file))


def _seed(store: HistoryStore, pipeline: str, statuses: list[str]) -> None:
    path = pathlib.Path(store._path)
    entries = [_entry(pipeline, s) for s in statuses]
    existing = json.loads(path.read_text()) if path.exists() else {}
    existing.setdefault(pipeline, [])
    existing[pipeline].extend(entries)
    path.write_text(json.dumps(existing))


# --- unit tests for _count_transitions ---

def test_count_transitions_empty():
    assert _count_transitions([]) == 0


def test_count_transitions_single():
    e = HistoryEntry.from_dict(_entry("p", "ok"))
    assert _count_transitions([e]) == 0


def test_count_transitions_all_same():
    entries = [HistoryEntry.from_dict(_entry("p", "ok")) for _ in range(5)]
    assert _count_transitions(entries) == 0


def test_count_transitions_alternating():
    statuses = ["ok", "stale", "ok", "stale", "ok"]
    entries = [HistoryEntry.from_dict(_entry("p", s)) for s in statuses]
    assert _count_transitions(entries) == 4


# --- check_flap ---

def test_no_history_returns_stable(store: HistoryStore):
    result = check_flap("pipe_a", store)
    assert not result.is_flapping
    assert result.transitions == 0


def test_stable_pipeline(store: HistoryStore, history_file: pathlib.Path):
    _seed(store, "pipe_a", ["ok"] * 8)
    result = check_flap("pipe_a", store, window=10, threshold=3)
    assert not result.is_flapping


def test_flapping_pipeline(store: HistoryStore, history_file: pathlib.Path):
    _seed(store, "pipe_a", ["ok", "stale", "ok", "stale", "ok", "failed"])
    result = check_flap("pipe_a", store, window=10, threshold=3)
    assert result.is_flapping
    assert result.transitions >= 3


def test_window_limits_entries(store: HistoryStore, history_file: pathlib.Path):
    # Many old stable runs followed by recent flapping — window should only see recent
    _seed(store, "pipe_a", ["ok"] * 20 + ["ok", "stale", "ok", "stale"])
    result = check_flap("pipe_a", store, window=4, threshold=3)
    assert result.window == 4
    assert result.is_flapping


def test_flap_result_summary_flapping():
    r = FlapResult(pipeline="my_pipe", transitions=4, window=10, is_flapping=True)
    assert "FLAPPING" in r.summary
    assert "my_pipe" in r.summary


def test_flap_result_summary_stable():
    r = FlapResult(pipeline="my_pipe", transitions=1, window=10, is_flapping=False)
    assert "stable" in r.summary


# --- check_all_flap ---

def test_check_all_flap_returns_one_per_pipeline(store: HistoryStore):
    results = check_all_flap(["a", "b", "c"], store)
    assert len(results) == 3
    assert {r.pipeline for r in results} == {"a", "b", "c"}
