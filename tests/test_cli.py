"""Tests for pipewatch CLI commands including the new report command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.cli import cli

CONFIG_YAML = """
state_file: /tmp/pw_test_state.json
pipelines:
  - name: ingest
    max_age_minutes: 60
    alert_on_failure: true
  - name: export
    max_age_minutes: 120
    alert_on_failure: false
"""


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    p = tmp_path / "pipewatch.yaml"
    p.write_text(CONFIG_YAML)
    return p


@pytest.fixture()
def ok_results() -> list[CheckResult]:
    return [
        CheckResult(pipeline_name="ingest", status=CheckStatus.OK, message=None),
        CheckResult(pipeline_name="export", status=CheckStatus.OK, message=None),
    ]


@pytest.fixture()
def mixed_results() -> list[CheckResult]:
    return [
        CheckResult(pipeline_name="ingest", status=CheckStatus.OK, message=None),
        CheckResult(pipeline_name="export", status=CheckStatus.STALE, message="overdue"),
    ]


# --- check ---

def test_check_all_ok(runner, config_file, ok_results):
    with patch("pipewatch.cli.check_pipeline", side_effect=ok_results):
        result = runner.invoke(cli, ["--config", str(config_file), "check"])
    assert result.exit_code == 0


def test_check_mixed_exits_nonzero(runner, config_file, mixed_results):
    with patch("pipewatch.cli.check_pipeline", side_effect=mixed_results):
        result = runner.invoke(cli, ["--config", str(config_file), "check"])
    assert result.exit_code == 1


# --- list ---

def test_list_shows_pipelines(runner, config_file):
    result = runner.invoke(cli, ["--config", str(config_file), "list"])
    assert result.exit_code == 0
    assert "ingest" in result.output
    assert "export" in result.output


# --- report ---

def test_report_text_format(runner, config_file, ok_results):
    with patch("pipewatch.cli.check_pipeline", side_effect=ok_results):
        result = runner.invoke(cli, ["--config", str(config_file), "report", "--format", "text"])
    assert result.exit_code == 0
    assert "PipeWatch Report" in result.output


def test_report_json_format(runner, config_file, ok_results):
    with patch("pipewatch.cli.check_pipeline", side_effect=ok_results):
        result = runner.invoke(cli, ["--config", str(config_file), "report", "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["summary"]["total"] == 2


def test_report_csv_format(runner, config_file, ok_results):
    with patch("pipewatch.cli.check_pipeline", side_effect=ok_results):
        result = runner.invoke(cli, ["--config", str(config_file), "report", "--format", "csv"])
    assert result.exit_code == 0
    assert "pipeline_name" in result.output


def test_report_writes_to_file(runner, config_file, ok_results, tmp_path):
    out_file = tmp_path / "report.txt"
    with patch("pipewatch.cli.check_pipeline", side_effect=ok_results):
        result = runner.invoke(
            cli,
            ["--config", str(config_file), "report", "--output", str(out_file)],
        )
    assert result.exit_code == 0
    assert out_file.exists()
    assert "PipeWatch Report" in out_file.read_text()
