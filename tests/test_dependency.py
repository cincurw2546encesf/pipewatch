"""Tests for pipewatch.dependency."""
from datetime import datetime, timezone
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.dependency import check_dependencies, DependencyReport


def _r(name: str, status: CheckStatus) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        status=status,
        last_run=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        message="ok" if status == CheckStatus.OK else "fail",
    )


def test_no_deps_returns_healthy():
    results = [_r("a", CheckStatus.OK)]
    report = check_dependencies(results, {})
    assert report.healthy
    assert report.checked == 0


def test_satisfied_dependency_passes():
    results = [_r("upstream", CheckStatus.OK), _r("downstream", CheckStatus.OK)]
    report = check_dependencies(results, {"downstream": ["upstream"]})
    assert report.healthy
    assert report.checked == 1


def test_stale_dependency_raises_violation():
    results = [_r("upstream", CheckStatus.STALE), _r("downstream", CheckStatus.OK)]
    report = check_dependencies(results, {"downstream": ["upstream"]})
    assert not report.healthy
    assert len(report.violations) == 1
    v = report.violations[0]
    assert v.pipeline == "downstream"
    assert v.depends_on == "upstream"
    assert "stale" in v.reason


def test_failed_dependency_raises_violation():
    results = [_r("upstream", CheckStatus.FAILED), _r("downstream", CheckStatus.OK)]
    report = check_dependencies(results, {"downstream": ["upstream"]})
    assert not report.healthy
    assert "failed" in report.violations[0].reason


def test_missing_dependency_raises_violation():
    results = [_r("downstream", CheckStatus.OK)]
    report = check_dependencies(results, {"downstream": ["upstream"]})
    assert not report.healthy
    assert "not found" in report.violations[0].reason


def test_multiple_deps_partial_failure():
    results = [
        _r("a", CheckStatus.OK),
        _r("b", CheckStatus.STALE),
        _r("c", CheckStatus.OK),
    ]
    report = check_dependencies(results, {"c": ["a", "b"]})
    assert not report.healthy
    assert report.checked == 2
    assert len(report.violations) == 1


def test_summary_healthy():
    results = [_r("up", CheckStatus.OK), _r("down", CheckStatus.OK)]
    report = check_dependencies(results, {"down": ["up"]})
    assert "passed" in report.summary()


def test_summary_violations():
    results = [_r("down", CheckStatus.OK)]
    report = check_dependencies(results, {"down": ["missing"]})
    assert "violation" in report.summary()
    assert "missing" in report.summary()


def test_str_violation():
    results = [_r("up", CheckStatus.FAILED), _r("down", CheckStatus.OK)]
    report = check_dependencies(results, {"down": ["up"]})
    s = str(report.violations[0])
    assert "down" in s
    assert "up" in s
