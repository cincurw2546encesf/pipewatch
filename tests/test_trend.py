"""Tests for pipewatch.trend."""
import pytest
from unittest.mock import MagicMock
from pipewatch.trend import analyse_trend, analyse_all, TrendResult, _trend_direction
from pipewatch.history import HistoryEntry
from pipewatch.checker import CheckStatus
import datetime


def _entry(status: str) -> HistoryEntry:
    return HistoryEntry(
        pipeline="pipe",
        status=status,
        checked_at=datetime.datetime.utcnow().isoformat(),
        last_run=None,
        message="",
    )


@pytest.fixture
def store():
    return MagicMock()


def test_analyse_trend_empty(store):
    store.get.return_value = []
    result = analyse_trend("pipe", store)
    assert result.total == 0
    assert result.trend == "insufficient_data"
    assert result.failure_rate == 0.0


def test_analyse_trend_all_ok(store):
    store.get.return_value = [_entry(CheckStatus.OK.value)] * 20
    result = analyse_trend("pipe", store)
    assert result.ok_count == 20
    assert result.failed_count == 0
    assert result.stale_count == 0
    assert result.failure_rate == 0.0


def test_analyse_trend_degrading(store):
    older = [_entry(CheckStatus.OK.value)] * 5
    newer = [_entry(CheckStatus.FAILED.value)] * 5
    store.get.return_value = older + newer
    result = analyse_trend("pipe", store)
    assert result.trend == "degrading"


def test_analyse_trend_improving(store):
    older = [_entry(CheckStatus.FAILED.value)] * 5
    newer = [_entry(CheckStatus.OK.value)] * 5
    store.get.return_value = older + newer
    result = analyse_trend("pipe", store)
    assert result.trend == "improving"


def test_analyse_trend_stable(store):
    entries = [_entry(CheckStatus.OK.value)] * 10
    store.get.return_value = entries
    result = analyse_trend("pipe", store)
    assert result.trend == "stable"


def test_failure_rate(store):
    store.get.return_value = (
        [_entry(CheckStatus.OK.value)] * 3
        + [_entry(CheckStatus.FAILED.value)] * 1
        + [_entry(CheckStatus.STALE.value)] * 1
    )
    result = analyse_trend("pipe", store)
    assert abs(result.failure_rate - 0.4) < 1e-6


def test_analyse_all_returns_one_per_pipeline(store):
    store.get.return_value = []
    results = analyse_all(["a", "b", "c"], store)
    assert len(results) == 3
    assert {r.pipeline for r in results} == {"a", "b", "c"}
