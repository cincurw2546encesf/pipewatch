"""Tests for pipewatch.sla."""
from __future__ import annotations

from datetime import datetime, time, timezone

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.sla import SLAConfig, SLAViolation, check_sla


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime(2024, 6, 3, 8, 0, 0, tzinfo=timezone.utc)  # Monday


def _result(name: str, status: CheckStatus, last_run: datetime | None = None) -> CheckResult:
    return CheckResult(
        pipeline=name,
        status=status,
        last_run=last_run,
        message="",
        checked_at=NOW,
    )


@pytest.fixture
def sla_cfg() -> SLAConfig:
    return SLAConfig(pipeline="pipe_a", deadline=time(6, 0))


# ---------------------------------------------------------------------------
# SLAConfig.applies_today
# ---------------------------------------------------------------------------

def test_applies_today_no_days(sla_cfg):
    assert sla_cfg.applies_today(NOW) is True


def test_applies_today_matching_day():
    cfg = SLAConfig(pipeline="p", deadline=time(6, 0), days=[1])  # Monday
    assert cfg.applies_today(NOW) is True


def test_applies_today_non_matching_day():
    cfg = SLAConfig(pipeline="p", deadline=time(6, 0), days=[2, 3])  # Tue/Wed
    assert cfg.applies_today(NOW) is False


# ---------------------------------------------------------------------------
# check_sla — skipped cases
# ---------------------------------------------------------------------------

def test_sla_skipped_before_deadline():
    """If current time is before the deadline, the SLA is skipped."""
    early = datetime(2024, 6, 3, 5, 0, 0, tzinfo=timezone.utc)
    cfg = SLAConfig(pipeline="pipe_a", deadline=time(6, 0))
    report = check_sla([cfg], [], now=early)
    assert "pipe_a" in report.skipped
    assert report.healthy


def test_sla_skipped_wrong_day():
    cfg = SLAConfig(pipeline="pipe_a", deadline=time(6, 0), days=[2])  # Tuesday only
    report = check_sla([cfg], [], now=NOW)  # NOW is Monday
    assert "pipe_a" in report.skipped


# ---------------------------------------------------------------------------
# check_sla — passing cases
# ---------------------------------------------------------------------------

def test_sla_passes_when_run_today():
    run_time = datetime(2024, 6, 3, 5, 30, 0, tzinfo=timezone.utc)
    cfg = SLAConfig(pipeline="pipe_a", deadline=time(6, 0))
    res = _result("pipe_a", CheckStatus.OK, last_run=run_time)
    report = check_sla([cfg], [res], now=NOW)
    assert "pipe_a" in report.passed
    assert report.healthy


# ---------------------------------------------------------------------------
# check_sla — violation cases
# ---------------------------------------------------------------------------

def test_sla_violated_no_result():
    cfg = SLAConfig(pipeline="pipe_a", deadline=time(6, 0))
    report = check_sla([cfg], [], now=NOW)
    assert len(report.violations) == 1
    assert report.violations[0].pipeline == "pipe_a"
    assert report.violations[0].last_success is None
    assert not report.healthy


def test_sla_violated_stale_status():
    run_time = datetime(2024, 6, 3, 5, 30, 0, tzinfo=timezone.utc)
    cfg = SLAConfig(pipeline="pipe_a", deadline=time(6, 0))
    res = _result("pipe_a", CheckStatus.STALE, last_run=run_time)
    report = check_sla([cfg], [res], now=NOW)
    assert len(report.violations) == 1


def test_sla_violated_run_yesterday():
    yesterday_run = datetime(2024, 6, 2, 5, 0, 0, tzinfo=timezone.utc)
    cfg = SLAConfig(pipeline="pipe_a", deadline=time(6, 0))
    res = _result("pipe_a", CheckStatus.OK, last_run=yesterday_run)
    report = check_sla([cfg], [res], now=NOW)
    assert len(report.violations) == 1


# ---------------------------------------------------------------------------
# SLAViolation.__str__
# ---------------------------------------------------------------------------

def test_violation_str_with_last_success():
    v = SLAViolation(
        pipeline="pipe_a",
        deadline=time(6, 0),
        last_success=datetime(2024, 6, 2, 5, 0, 0, tzinfo=timezone.utc),
        evaluated_at=NOW,
    )
    assert "pipe_a" in str(v)
    assert "06:00:00" in str(v)


def test_violation_str_no_last_success():
    v = SLAViolation(
        pipeline="pipe_a",
        deadline=time(6, 0),
        last_success=None,
        evaluated_at=NOW,
    )
    assert "never" in str(v)


# ---------------------------------------------------------------------------
# SLAReport.summary
# ---------------------------------------------------------------------------

def test_report_summary_mixed():
    cfg_a = SLAConfig(pipeline="a", deadline=time(6, 0))
    cfg_b = SLAConfig(pipeline="b", deadline=time(6, 0))
    run_ok = datetime(2024, 6, 3, 5, 0, 0, tzinfo=timezone.utc)
    results = [
        _result("a", CheckStatus.OK, last_run=run_ok),
    ]
    report = check_sla([cfg_a, cfg_b], results, now=NOW)
    summary = report.summary()
    assert "1 passed" in summary
    assert "1 violated" in summary
