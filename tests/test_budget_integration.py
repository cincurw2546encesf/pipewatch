"""Integration test: budget config parsed from TOML and checked end-to-end."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipewatch.budget import BudgetConfig, check_all_budgets
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import load_config


@pytest.fixture()
def tmp_config(tmp_path: Path) -> Path:
    return tmp_path / "config.toml"


def _write(p: Path, text: str) -> None:
    p.write_text(text)


def _utc(h: int, m: int, s: int) -> datetime:
    return datetime(2024, 6, 1, h, m, s, tzinfo=timezone.utc)


def _mock_result(name: str, start: datetime | None, end: datetime | None) -> CheckResult:
    r = MagicMock(spec=CheckResult)
    r.pipeline = name
    r.status = CheckStatus.OK
    r.last_run = start
    r.last_finished = end
    return r


def test_budget_config_parsed_from_toml(tmp_config: Path) -> None:
    _write(
        tmp_config,
        """
[app]
state_file = "/tmp/state.json"

[[pipelines]]
name = "etl_daily"
max_age_seconds = 86400
max_runtime_seconds = 300

[[pipelines]]
name = "etl_hourly"
max_age_seconds = 3600
""",
    )
    cfg = load_config(str(tmp_config))
    daily = next(p for p in cfg.pipelines if p.name == "etl_daily")
    hourly = next(p for p in cfg.pipelines if p.name == "etl_hourly")
    assert daily.max_runtime_seconds == 300
    assert getattr(hourly, "max_runtime_seconds", None) is None


def test_no_budget_violation_when_within_limit(tmp_config: Path) -> None:
    budgets = [BudgetConfig(pipeline="etl_daily", max_seconds=300)]
    r = _mock_result("etl_daily", _utc(10, 0, 0), _utc(10, 4, 0))
    results = check_all_budgets([r], budgets)
    assert len(results) == 1
    assert results[0].exceeded is False


def test_budget_violation_detected(tmp_config: Path) -> None:
    budgets = [BudgetConfig(pipeline="etl_daily", max_seconds=60)]
    r = _mock_result("etl_daily", _utc(10, 0, 0), _utc(10, 5, 0))
    results = check_all_budgets([r], budgets)
    assert results[0].exceeded is True
    assert results[0].actual_seconds == pytest.approx(300.0)
