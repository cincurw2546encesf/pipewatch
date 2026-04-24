"""Tests for pipewatch.watchdog_cmd."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.watchdog import WatchdogReport, WatchdogEntry
from pipewatch.watchdog_cmd import watchdog_cmd


@pytest.fixture()
def runner():
    return CliRunner()


def _make_cfg():
    return AppConfig(
        pipelines=[PipelineConfig(name="pipe_a", max_age_minutes=60, schedule="")],
        state_file="state.json",
    )


def _invoke(runner, report, fmt="text"):
    cfg = _make_cfg()
    with patch("pipewatch.watchdog_cmd.StateStore"), \
         patch("pipewatch.watchdog_cmd.run_watchdog", return_value=report):
        result = runner.invoke(
            watchdog_cmd,
            ["check", "--format", fmt],
            obj={"config": cfg},
            catch_exceptions=False,
        )
    return result


def test_check_healthy_exits_zero(runner):
    from datetime import datetime, timezone
    report = WatchdogReport(generated_at=datetime.now(timezone.utc), entries=[])
    result = _invoke(runner, report)
    assert result.exit_code == 0
    assert "all pipelines" in result.output


def test_check_issues_exits_nonzero(runner):
    from datetime import datetime, timezone
    entry = WatchdogEntry(pipeline="pipe_a", issue="never_run")
    report = WatchdogReport(
        generated_at=datetime.now(timezone.utc), entries=[entry]
    )
    result = _invoke(runner, report)
    assert result.exit_code != 0
    assert "pipe_a" in result.output


def test_check_json_format_healthy(runner):
    from datetime import datetime, timezone
    report = WatchdogReport(generated_at=datetime.now(timezone.utc), entries=[])
    result = _invoke(runner, report, fmt="json")
    data = json.loads(result.output)
    assert data["healthy"] is True
    assert data["issues"] == []


def test_check_json_format_with_issue(runner):
    from datetime import datetime, timezone
    entry = WatchdogEntry(pipeline="pipe_a", issue="stale_deadline", note="too old")
    report = WatchdogReport(
        generated_at=datetime.now(timezone.utc), entries=[entry]
    )
    result = _invoke(runner, report, fmt="json")
    data = json.loads(result.output)
    assert data["healthy"] is False
    assert data["issues"][0]["pipeline"] == "pipe_a"
    assert data["issues"][0]["issue"] == "stale_deadline"
