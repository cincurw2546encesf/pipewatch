"""Tests for pipewatch.sla_cmd."""
from __future__ import annotations

from datetime import datetime, time, timezone
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.sla import SLAConfig
from pipewatch.sla_cmd import sla_cmd


NOW = datetime(2024, 6, 3, 8, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def runner():
    return CliRunner()


def _make_pipeline(name: str, sla_raw: dict | None = None):
    p = MagicMock()
    p.name = name
    p.sla = sla_raw
    return p


def _make_app_cfg(pipelines):
    cfg = MagicMock()
    cfg.pipelines = pipelines
    return cfg


def _ok_result(name: str) -> CheckResult:
    return CheckResult(
        pipeline=name,
        status=CheckStatus.OK,
        last_run=datetime(2024, 6, 3, 5, 0, 0, tzinfo=timezone.utc),
        message="ok",
        checked_at=NOW,
    )


# ---------------------------------------------------------------------------
# list command
# ---------------------------------------------------------------------------

def test_list_no_slas(runner):
    cfg = _make_app_cfg([_make_pipeline("pipe_a", sla_raw=None)])
    result = runner.invoke(sla_cmd, ["list"], obj={"config": cfg})
    assert result.exit_code == 0
    assert "No SLA" in result.output


def test_list_shows_sla_entry(runner):
    cfg = _make_app_cfg([
        _make_pipeline("pipe_a", sla_raw={"deadline": "06:00", "days": [1, 2, 3]})
    ])
    result = runner.invoke(sla_cmd, ["list"], obj={"config": cfg})
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "06:00" in result.output


def test_list_no_config(runner):
    result = runner.invoke(sla_cmd, ["list"], obj={})
    assert result.exit_code != 0 or "No config" in result.output


# ---------------------------------------------------------------------------
# check command
# ---------------------------------------------------------------------------

def test_check_no_slas(runner):
    cfg = _make_app_cfg([_make_pipeline("pipe_a", sla_raw=None)])
    result = runner.invoke(sla_cmd, ["check"], obj={"config": cfg, "results": []})
    assert result.exit_code == 0
    assert "No SLA" in result.output


def test_check_all_passing(runner, monkeypatch):
    import pipewatch.sla as sla_mod
    monkeypatch.setattr(sla_mod, "_utcnow", lambda: NOW)

    cfg = _make_app_cfg([
        _make_pipeline("pipe_a", sla_raw={"deadline": "06:00"})
    ])
    results = [_ok_result("pipe_a")]
    result = runner.invoke(sla_cmd, ["check"], obj={"config": cfg, "results": results})
    assert result.exit_code == 0
    assert "passed" in result.output


def test_check_violation_exits_2(runner, monkeypatch):
    import pipewatch.sla as sla_mod
    monkeypatch.setattr(sla_mod, "_utcnow", lambda: NOW)

    cfg = _make_app_cfg([
        _make_pipeline("pipe_a", sla_raw={"deadline": "06:00"})
    ])
    # No results => violation
    result = runner.invoke(sla_cmd, ["check"], obj={"config": cfg, "results": []})
    assert result.exit_code == 2
    assert "violated" in result.output
