"""Tests for pipewatch.expiry."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.expiry import ExpiryResult, check_expiry, check_all_expiry


UTC = timezone.utc
NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _now_fn():
    return NOW


def _pipeline(name="pipe", max_age_seconds=3600):
    p = MagicMock()
    p.name = name
    p.max_age_seconds = max_age_seconds
    return p


def _result(pipeline="pipe", status=CheckStatus.OK, last_run=None):
    r = MagicMock(spec=CheckResult)
    r.pipeline = pipeline
    r.status = status
    r.last_run = last_run
    return r


def test_no_max_age_returns_none():
    p = _pipeline(max_age_seconds=None)
    r = _result()
    assert check_expiry(p, r, now_fn=_now_fn) is None


def test_ok_run_within_limit_not_expired():
    last_run = NOW - timedelta(seconds=1800)
    p = _pipeline(max_age_seconds=3600)
    r = _result(status=CheckStatus.OK, last_run=last_run)
    er = check_expiry(p, r, now_fn=_now_fn)
    assert er is not None
    assert not er.expired
    assert er.last_ok_seconds_ago == pytest.approx(1800.0)


def test_ok_run_past_limit_is_expired():
    last_run = NOW - timedelta(seconds=7200)
    p = _pipeline(max_age_seconds=3600)
    r = _result(status=CheckStatus.OK, last_run=last_run)
    er = check_expiry(p, r, now_fn=_now_fn)
    assert er is not None
    assert er.expired


def test_no_last_run_is_expired():
    p = _pipeline(max_age_seconds=3600)
    r = _result(status=CheckStatus.OK, last_run=None)
    er = check_expiry(p, r, now_fn=_now_fn)
    assert er is not None
    assert er.expired
    assert er.last_ok_seconds_ago is None


def test_non_ok_status_treated_as_unknown():
    last_run = NOW - timedelta(seconds=100)
    p = _pipeline(max_age_seconds=3600)
    r = _result(status=CheckStatus.STALE, last_run=last_run)
    er = check_expiry(p, r, now_fn=_now_fn)
    assert er is not None
    assert er.expired  # unknown last_ok => expired
    assert er.last_ok_seconds_ago is None


def test_summary_no_expiry():
    er = ExpiryResult(pipeline="p", max_age_seconds=None, last_ok_seconds_ago=None, expired=False)
    assert "no expiry" in er.summary()


def test_summary_never_run():
    er = ExpiryResult(pipeline="p", max_age_seconds=3600, last_ok_seconds_ago=None, expired=True)
    assert "never run" in er.summary()


def test_summary_expired():
    er = ExpiryResult(pipeline="p", max_age_seconds=3600, last_ok_seconds_ago=7200.0, expired=True)
    assert "EXPIRED" in er.summary()


def test_summary_ok():
    er = ExpiryResult(pipeline="p", max_age_seconds=3600, last_ok_seconds_ago=1800.0, expired=False)
    assert "ok" in er.summary()


def test_check_all_expiry_filters_no_config():
    p1 = _pipeline(name="a", max_age_seconds=3600)
    p2 = _pipeline(name="b", max_age_seconds=None)
    r1 = _result(pipeline="a", status=CheckStatus.OK, last_run=NOW - timedelta(seconds=100))
    r2 = _result(pipeline="b", status=CheckStatus.OK, last_run=NOW - timedelta(seconds=100))
    results = check_all_expiry([p1, p2], [r1, r2], now_fn=_now_fn)
    assert len(results) == 1
    assert results[0].pipeline == "a"


def test_check_all_expiry_missing_result_skipped():
    p1 = _pipeline(name="a", max_age_seconds=3600)
    results = check_all_expiry([p1], [], now_fn=_now_fn)
    assert results == []
