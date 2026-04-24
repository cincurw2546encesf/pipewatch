"""Integration-style tests: SLA configs parsed from AppConfig feed into check_sla."""
from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import load_config
from pipewatch.sla import SLAConfig, check_sla
from pipewatch.sla_cmd import _build_sla_configs


NOW = datetime(2024, 6, 3, 9, 0, 0, tzinfo=timezone.utc)  # Monday, after 06:00


@pytest.fixture
def tmp_config(tmp_path: Path):
    def _write(content: str) -> Path:
        p = tmp_path / "config.toml"
        p.write_text(content)
        return p
    return _write


VALID_TOML = """
[app]
state_file = "/tmp/pw_state.json"

[[pipelines]]
name = "etl_daily"
max_age_minutes = 120

[pipelines.sla]
deadline = "06:00"
days = [1, 2, 3, 4, 5]

[[pipelines]]
name = "etl_weekly"
max_age_minutes = 1440
"""


def test_sla_config_parsed_from_toml(tmp_config):
    path = tmp_config(VALID_TOML)
    app_cfg = load_config(path)
    sla_configs = _build_sla_configs(app_cfg)
    assert len(sla_configs) == 1
    assert sla_configs[0].pipeline == "etl_daily"
    assert sla_configs[0].deadline == time(6, 0)
    assert sla_configs[0].days == [1, 2, 3, 4, 5]


def test_sla_no_config_for_pipeline_without_sla(tmp_config):
    path = tmp_config(VALID_TOML)
    app_cfg = load_config(path)
    sla_configs = _build_sla_configs(app_cfg)
    names = [c.pipeline for c in sla_configs]
    assert "etl_weekly" not in names


def test_end_to_end_violation(tmp_config):
    path = tmp_config(VALID_TOML)
    app_cfg = load_config(path)
    sla_configs = _build_sla_configs(app_cfg)
    # No results supplied — should trigger violation after deadline
    report = check_sla(sla_configs, [], now=NOW)
    assert not report.healthy
    assert report.violations[0].pipeline == "etl_daily"


def test_end_to_end_pass(tmp_config):
    path = tmp_config(VALID_TOML)
    app_cfg = load_config(path)
    sla_configs = _build_sla_configs(app_cfg)
    run_time = datetime(2024, 6, 3, 5, 45, 0, tzinfo=timezone.utc)
    results = [
        CheckResult(
            pipeline="etl_daily",
            status=CheckStatus.OK,
            last_run=run_time,
            message="ok",
            checked_at=NOW,
        )
    ]
    report = check_sla(sla_configs, results, now=NOW)
    assert report.healthy
    assert "etl_daily" in report.passed


def test_sla_skipped_on_weekend(tmp_config):
    """etl_daily has days=[1..5]; Saturday should be skipped."""
    saturday = datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc)  # Saturday
    path = tmp_config(VALID_TOML)
    app_cfg = load_config(path)
    sla_configs = _build_sla_configs(app_cfg)
    report = check_sla(sla_configs, [], now=saturday)
    assert "etl_daily" in report.skipped
    assert report.healthy
