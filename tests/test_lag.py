"""Tests for pipewatch.lag."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import PipelineConfig
from pipewatch.lag import LagResult, compute_lag, check_all_lag, lag_summary


FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _now_fn():
    return FIXED_NOW


def _pipeline(name="pipe", max_age=3600) -> PipelineConfig:
    return PipelineConfig(name=name, max_age_seconds=max_age, tags=[])


def _result(pipeline, status=CheckStatus.OK, last_run=None) -> CheckResult:
    return CheckResult(pipeline=pipeline, status=status, last_run=last_run, message="")


# --- compute_lag ---

def test_lag_no_last_run_returns_none_seconds():
    r = _result(_pipeline(), status=CheckStatus.MISSING)
    lag = compute_lag(r, now_fn=_now_fn)
    assert lag.lag_seconds is None
    assert lag.last_run is None


def test_lag_no_last_run_exceeded_when_not_ok():
    r = _result(_pipeline(), status=CheckStatus.MISSING)
    lag = compute_lag(r, now_fn=_now_fn)
    assert lag.exceeded is True


def test_lag_no_last_run_not_exceeded_when_ok():
    r = _result(_pipeline(), status=CheckStatus.OK)
    lag = compute_lag(r, now_fn=_now_fn)
    assert lag.exceeded is False


def test_lag_within_interval_not_exceeded():
    last_run = FIXED_NOW - timedelta(seconds=1800)  # 30 min ago, limit 60 min
    r = _result(_pipeline(max_age=3600), status=CheckStatus.OK, last_run=last_run)
    lag = compute_lag(r, now_fn=_now_fn)
    assert lag.lag_seconds == pytest.approx(1800.0)
    assert lag.exceeded is False


def test_lag_past_interval_exceeded():
    last_run = FIXED_NOW - timedelta(seconds=7200)  # 2h ago, limit 1h
    r = _result(_pipeline(max_age=3600), status=CheckStatus.STALE, last_run=last_run)
    lag = compute_lag(r, now_fn=_now_fn)
    assert lag.lag_seconds == pytest.approx(7200.0)
    assert lag.exceeded is True


def test_lag_naive_datetime_treated_as_utc():
    naive = datetime(2024, 6, 1, 11, 0, 0)  # no tzinfo
    r = _result(_pipeline(max_age=3600), status=CheckStatus.OK, last_run=naive)
    lag = compute_lag(r, now_fn=_now_fn)
    assert lag.lag_seconds == pytest.approx(3600.0)


def test_lag_result_summary_no_last_run():
    r = _result(_pipeline(name="alpha"), status=CheckStatus.MISSING)
    lag = compute_lag(r, now_fn=_now_fn)
    assert "alpha" in lag.summary
    assert "never run" in lag.summary


def test_lag_result_summary_exceeded():
    last_run = FIXED_NOW - timedelta(seconds=7200)
    r = _result(_pipeline(name="beta", max_age=3600), status=CheckStatus.STALE, last_run=last_run)
    lag = compute_lag(r, now_fn=_now_fn)
    assert "EXCEEDED" in lag.summary
    assert "beta" in lag.summary


def test_lag_result_summary_ok():
    last_run = FIXED_NOW - timedelta(seconds=600)
    r = _result(_pipeline(name="gamma", max_age=3600), status=CheckStatus.OK, last_run=last_run)
    lag = compute_lag(r, now_fn=_now_fn)
    assert "ok" in lag.summary


# --- check_all_lag ---

def test_check_all_lag_returns_one_per_pipeline():
    pipelines = [_pipeline(name=f"p{i}") for i in range(4)]
    results = [_result(p) for p in pipelines]
    lags = check_all_lag(results, now_fn=_now_fn)
    assert len(lags) == 4


def test_check_all_lag_empty():
    assert check_all_lag([], now_fn=_now_fn) == []


# --- lag_summary ---

def test_lag_summary_contains_pipeline_names():
    pipelines = [_pipeline(name="aaa"), _pipeline(name="bbb")]
    results = [_result(p) for p in pipelines]
    lags = check_all_lag(results, now_fn=_now_fn)
    text = lag_summary(lags)
    assert "aaa" in text
    assert "bbb" in text


def test_lag_summary_shows_count():
    results = [_result(_pipeline(name=f"p{i}")) for i in range(3)]
    lags = check_all_lag(results, now_fn=_now_fn)
    text = lag_summary(lags)
    assert "3 pipelines" in text
