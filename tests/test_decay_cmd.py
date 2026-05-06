"""Tests for pipewatch.decay_cmd CLI commands."""
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.decay import DecayStore
from pipewatch.decay_cmd import decay_cmd


@pytest.fixture()
def runner():
    return CliRunner()


class _Pipeline:
    def __init__(self, name, decay=None):
        self.name = name
        self.decay = decay


class _AppCfg:
    def __init__(self, state_dir, pipelines=None):
        self.state_dir = str(state_dir)
        self.pipelines = pipelines or []


@pytest.fixture()
def ctx_obj(tmp_path):
    cfg = _AppCfg(tmp_path, pipelines=[
        _Pipeline("alpha", decay={"threshold": 0.5, "half_life_days": 7}),
        _Pipeline("beta"),
    ])
    return {"app_cfg": cfg}


def invoke(runner, ctx_obj, *args):
    return runner.invoke(decay_cmd, list(args), obj=ctx_obj, catch_exceptions=False)


def test_list_no_entries(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No decay entries" in result.output


def test_list_shows_entry(runner, ctx_obj, tmp_path):
    store = DecayStore(tmp_path / "decay.json")
    store.record_failure("alpha")
    result = invoke(runner, ctx_obj, "list")
    assert "alpha" in result.output
    assert "failures=1" in result.output


def test_reset_removes_entry(runner, ctx_obj, tmp_path):
    store = DecayStore(tmp_path / "decay.json")
    store.record_failure("alpha")
    result = invoke(runner, ctx_obj, "reset", "alpha")
    assert result.exit_code == 0
    assert "reset" in result.output.lower()
    assert DecayStore(tmp_path / "decay.json").get("alpha").failure_count == 0


def test_check_no_decay_config_exits_zero(runner, ctx_obj):
    # beta has no decay config; alpha has config but 0 failures
    result = invoke(runner, ctx_obj, "check")
    assert result.exit_code == 0


def test_check_exceeded_exits_nonzero(runner, ctx_obj, tmp_path):
    store = DecayStore(tmp_path / "decay.json")
    now = datetime.now(timezone.utc)
    for _ in range(20):
        store.record_failure("alpha", now_fn=lambda: now)
    # Patch threshold to be very low so it's definitely exceeded
    ctx_obj["app_cfg"].pipelines = [
        _Pipeline("alpha", decay={"threshold": 0.001, "half_life_days": 7})
    ]
    result = invoke(runner, ctx_obj, "check")
    assert result.exit_code == 1
