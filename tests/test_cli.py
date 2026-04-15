"""Tests for pipewatch.cli."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.cli import cli
from pipewatch.checker import CheckResult, CheckStatus


SAMPLE_CONFIG = """
state_path: /tmp/pipewatch_test_state.json
pipelines:
  - name: orders_etl
    max_age_seconds: 3600
  - name: inventory_sync
    max_age_seconds: 7200
"""


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def config_file(tmp_path):
    cfg = tmp_path / "pipewatch.yaml"
    cfg.write_text(SAMPLE_CONFIG)
    return cfg


@pytest.fixture()
def ok_results():
    ts = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    return [
        CheckResult("orders_etl", CheckStatus.OK, "Healthy.", ts),
        CheckResult("inventory_sync", CheckStatus.OK, "Healthy.", ts),
    ]


@pytest.fixture()
def mixed_results():
    ts = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    return [
        CheckResult("orders_etl", CheckStatus.OK, "Healthy.", ts),
        CheckResult("inventory_sync", CheckStatus.STALE, "Too old.", ts),
    ]


def test_check_all_ok(runner, config_file, ok_results):
    with patch("pipewatch.cli.check_all", return_value=ok_results):
        result = runner.invoke(cli, ["--config", str(config_file), "check"])
    assert result.exit_code == 0
    assert "OK" in result.output


def test_check_unhealthy_exits_1(runner, config_file, mixed_results):
    with patch("pipewatch.cli.check_all", return_value=mixed_results):
        result = runner.invoke(cli, ["--config", str(config_file), "check"])
    assert result.exit_code == 1
    assert "STALE" in result.output


def test_check_with_alert_dispatches(runner, config_file, mixed_results):
    with patch("pipewatch.cli.check_all", return_value=mixed_results), \
         patch("pipewatch.cli.dispatch_alerts", return_value=[mixed_results[1]]) as mock_dispatch:
        result = runner.invoke(cli, ["--config", str(config_file), "check", "--alert"])
    assert result.exit_code == 1
    mock_dispatch.assert_called_once()


def test_check_ok_no_alert(runner, config_file, ok_results):
    with patch("pipewatch.cli.check_all", return_value=ok_results), \
         patch("pipewatch.cli.dispatch_alerts") as mock_dispatch:
        result = runner.invoke(cli, ["--config", str(config_file), "check", "--alert"])
    assert result.exit_code == 0
    mock_dispatch.assert_not_called()


def test_list_pipelines(runner, config_file):
    result = runner.invoke(cli, ["--config", str(config_file), "list"])
    assert result.exit_code == 0
    assert "orders_etl" in result.output
    assert "inventory_sync" in result.output
