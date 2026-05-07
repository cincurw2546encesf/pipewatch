"""Tests for pipewatch.liveliness."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.checker import CheckStatus
from pipewatch.liveliness import check_liveliness, check_all_liveliness, LivelinessResult


UTC = timezone.utc
NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(history_file)


def _add(store: HistoryStore, pipeline: str, finished_at: datetime) -> None:
    entry = HistoryEntry(
        pipeline=pipeline,
        status=CheckStatus.OK,
        started_at=finished_at - timedelta(minutes=1),
        finished_at=finished_at,
        duration_seconds=60.0,
    )
    store.append(entry)


def _now():
    return NOW


def test_no_max_silence_returns_none(store):
    result = check_liveliness("pipe", store, max_silence_seconds=0, now_fn=_now)
    assert result is None


def test_never_reported_is_exceeded(store):
    result = check_liveliness("pipe", store, max_silence_seconds=3600, now_fn=_now)
    assert result is not None
    assert result.exceeded is True
    assert result.last_seen is None
    assert result.seconds_since is None


def test_recent_run_not_exceeded(store):
    _add(store, "pipe", NOW - timedelta(minutes=30))
    result = check_liveliness("pipe", store, max_silence_seconds=3600, now_fn=_now)
    assert result is not None
    assert result.exceeded is False
    assert result.seconds_since == pytest.approx(1800, abs=1)


def test_old_run_exceeded(store):
    _add(store, "pipe", NOW - timedelta(hours=3))
    result = check_liveliness("pipe", store, max_silence_seconds=3600, now_fn=_now)
    assert result is not None
    assert result.exceeded is True


def test_exactly_at_boundary_not_exceeded(store):
    _add(store, "pipe", NOW - timedelta(seconds=3600))
    result = check_liveliness("pipe", store, max_silence_seconds=3600, now_fn=_now)
    assert result is not None
    assert result.exceeded is False


def test_summary_alive(store):
    _add(store, "pipe", NOW - timedelta(minutes=10))
    result = check_liveliness("pipe", store, max_silence_seconds=3600, now_fn=_now)
    assert "alive" in result.summary()
    assert "pipe" in result.summary()


def test_summary_dead(store):
    result = check_liveliness("pipe", store, max_silence_seconds=3600, now_fn=_now)
    assert "never reported" in result.summary()


class _Pipeline:
    def __init__(self, name, max_silence_seconds=0):
        self.name = name
        self.max_silence_seconds = max_silence_seconds


def test_check_all_skips_pipelines_without_limit(store):
    pipelines = [_Pipeline("a"), _Pipeline("b", 0)]
    results = check_all_liveliness(pipelines, store, now_fn=_now)
    assert results == []


def test_check_all_returns_results_for_configured_pipelines(store):
    _add(store, "a", NOW - timedelta(minutes=5))
    pipelines = [_Pipeline("a", 3600), _Pipeline("b", 7200)]
    results = check_all_liveliness(pipelines, store, now_fn=_now)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}
