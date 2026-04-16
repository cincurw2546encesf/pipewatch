"""Tests for pipewatch.dashboard."""
from datetime import datetime, timezone
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.dashboard import build_dashboard, summarise


def _r(name: str, status: CheckStatus, msg: str = "") -> CheckResult:
    last = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
    return CheckResult(pipeline=name, status=status, last_run=last, message=msg)


def _missing(name: str) -> CheckResult:
    return CheckResult(pipeline=name, status=CheckStatus.MISSING, last_run=None, message="no runs")


def test_summarise_all_ok():
    results = [_r("a", CheckStatus.OK), _r("b", CheckStatus.OK)]
    s = summarise(results)
    assert s.total == 2
    assert s.ok == 2
    assert s.stale == 0
    assert s.healthy is True


def test_summarise_mixed():
    results = [
        _r("a", CheckStatus.OK),
        _r("b", CheckStatus.STALE),
        _r("c", CheckStatus.FAILED),
        _missing("d"),
    ]
    s = summarise(results)
    assert s.total == 4
    assert s.ok == 1
    assert s.stale == 1
    assert s.failed == 1
    assert s.missing == 1
    assert s.healthy is False


def test_build_dashboard_contains_names():
    results = [_r("pipe-alpha", CheckStatus.OK), _r("pipe-beta", CheckStatus.STALE)]
    out = build_dashboard(results)
    assert "pipe-alpha" in out
    assert "pipe-beta" in out


def test_build_dashboard_shows_date():
    results = [_r("p", CheckStatus.OK)]
    out = build_dashboard(results)
    assert "2024-01-15" in out


def test_build_dashboard_missing_shows_never():
    results = [_missing("ghost")]
    out = build_dashboard(results)
    assert "never" in out


def test_build_dashboard_healthy_label():
    results = [_r("a", CheckStatus.OK)]
    out = build_dashboard(results)
    assert "HEALTHY" in out


def test_build_dashboard_degraded_label():
    results = [_r("a", CheckStatus.FAILED, "oops")]
    out = build_dashboard(results)
    assert "DEGRADED" in out
    assert "oops" in out


def test_build_dashboard_summary_counts():
    results = [_r("a", CheckStatus.OK), _r("b", CheckStatus.STALE)]
    out = build_dashboard(results)
    assert "Total: 2" in out
    assert "Stale: 1" in out
