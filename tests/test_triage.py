"""Tests for pipewatch.triage."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.triage import TriageEntry, TriageReport, triage, _score


def _utc(year=2024, month=1, day=1, hour=0):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


@pytest.fixture
def ok_result():
    return CheckResult(
        pipeline="pipe_ok",
        status=CheckStatus.OK,
        message="All good",
        last_run=_utc(hour=1),
    )


@pytest.fixture
def stale_result():
    return CheckResult(
        pipeline="pipe_stale",
        status=CheckStatus.STALE,
        message="Last run too old",
        last_run=_utc(hour=0),
    )


@pytest.fixture
def failed_result():
    return CheckResult(
        pipeline="pipe_failed",
        status=CheckStatus.FAILED,
        message="Run failed",
        last_run=_utc(hour=0),
    )


@pytest.fixture
def never_failed():
    return CheckResult(
        pipeline="pipe_never",
        status=CheckStatus.FAILED,
        message="Never ran",
        last_run=None,
    )


def test_triage_empty_results():
    report = triage([])
    assert report.entries == []
    assert report.healthy is True
    assert report.top is None


def test_triage_all_ok(ok_result):
    report = triage([ok_result])
    assert report.healthy is True
    assert len(report.critical()) == 0
    assert len(report.actionable()) == 0


def test_triage_orders_failed_before_stale(ok_result, stale_result, failed_result):
    report = triage([ok_result, stale_result, failed_result])
    assert report.entries[0].pipeline == "pipe_failed"
    assert report.entries[1].pipeline == "pipe_stale"
    assert report.entries[2].pipeline == "pipe_ok"


def test_triage_top_is_most_urgent(failed_result, stale_result):
    report = triage([stale_result, failed_result])
    assert report.top is not None
    assert report.top.pipeline == "pipe_failed"


def test_score_failed_higher_than_stale(failed_result, stale_result):
    assert _score(failed_result) > _score(stale_result)


def test_score_stale_higher_than_ok(stale_result, ok_result):
    assert _score(stale_result) > _score(ok_result)


def test_score_never_run_failed_boosted(never_failed, failed_result):
    assert _score(never_failed) > _score(failed_result)


def test_triage_summary_counts(ok_result, stale_result, failed_result):
    report = triage([ok_result, stale_result, failed_result])
    summary = report.summary()
    assert "3 pipeline" in summary
    assert "1 failed" in summary
    assert "1 stale" in summary


def test_triage_entry_summary_icons(failed_result, stale_result, ok_result):
    for result, expected_icon in [
        (failed_result, "🔴"),
        (stale_result, "🟡"),
        (ok_result, "🟢"),
    ]:
        entry = TriageEntry(result=result, score=_score(result))
        assert expected_icon in entry.summary()


def test_actionable_excludes_ok(ok_result, stale_result):
    report = triage([ok_result, stale_result])
    actionable = report.actionable()
    assert all(e.status != CheckStatus.OK for e in actionable)
    assert len(actionable) == 1
