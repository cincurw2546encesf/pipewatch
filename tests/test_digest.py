"""Tests for pipewatch.digest."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.trend import TrendResult
from pipewatch.digest import build_digest, format_digest_text, DigestReport

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _r(name, status, last_run=_NOW):
    r = MagicMock(spec=CheckResult)
    r.pipeline = name
    r.status = status
    r.last_run = last_run
    return r


def test_build_digest_counts():
    results = [
        _r("a", CheckStatus.OK),
        _r("b", CheckStatus.STALE),
        _r("c", CheckStatus.FAILED),
    ]
    report = build_digest(results, now_fn=lambda: _NOW)
    assert report.total == 3
    assert report.healthy == 1
    assert report.stale == 1
    assert report.failed == 1


def test_build_digest_no_trends():
    results = [_r("pipe", CheckStatus.OK)]
    report = build_digest(results, now_fn=lambda: _NOW)
    assert report.entries[0].failure_rate == 0.0
    assert report.entries[0].trend == "stable"


def test_build_digest_with_trend():
    tr = MagicMock(spec=TrendResult)
    tr.failure_rate = 0.5
    tr.direction = "degrading"
    results = [_r("pipe", CheckStatus.FAILED)]
    report = build_digest(results, trends={"pipe": tr}, now_fn=lambda: _NOW)
    assert report.entries[0].failure_rate == 0.5
    assert report.entries[0].trend == "degrading"


def test_build_digest_null_last_run():
    results = [_r("pipe", CheckStatus.STALE, last_run=None)]
    report = build_digest(results, now_fn=lambda: _NOW)
    assert report.entries[0].last_run is None


def test_summary_line():
    report = DigestReport(
        generated_at="2024-06-01T12:00:00Z",
        total=3, healthy=1, stale=1, failed=1,
    )
    line = report.summary_line
    assert "3 pipelines" in line
    assert "1 OK" in line
    assert "1 stale" in line
    assert "1 failed" in line


def test_format_digest_text_contains_names():
    results = [_r("alpha", CheckStatus.OK), _r("beta", CheckStatus.FAILED)]
    report = build_digest(results, now_fn=lambda: _NOW)
    text = format_digest_text(report)
    assert "alpha" in text
    assert "beta" in text


def test_format_digest_text_icons():
    results = [
        _r("a", CheckStatus.OK),
        _r("b", CheckStatus.STALE),
        _r("c", CheckStatus.FAILED),
    ]
    report = build_digest(results, now_fn=lambda: _NOW)
    text = format_digest_text(report)
    assert "✓" in text
    assert "~" in text
    assert "✗" in text
