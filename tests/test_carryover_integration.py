"""Integration tests: carryover detection via config + state."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.carryover import check_all_carryover
from pipewatch.config import load_config
from pipewatch.state import StateStore


DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def tmp_config(tmp_path):
    return tmp_path / "pipewatch.toml"


def _write(path, content):
    path.write_text(content)


def test_carryover_config_parsed_from_toml(tmp_config, tmp_path):
    _write(tmp_config, """
[app]
state_file = "state.json"

[[pipelines]]
name = "etl_daily"
schedule = "@daily"
max_staleness_seconds = 86400
max_carryover_seconds = 1800
""")
    app_cfg = load_config(str(tmp_config))
    p = app_cfg.pipelines[0]
    assert getattr(p, "max_carryover_seconds", None) == 1800


def test_no_carryover_config_gives_none_limit(tmp_config, tmp_path):
    _write(tmp_config, """
[app]
state_file = "state.json"

[[pipelines]]
name = "etl_daily"
schedule = "@daily"
max_staleness_seconds = 86400
""")
    app_cfg = load_config(str(tmp_config))
    p = app_cfg.pipelines[0]
    assert getattr(p, "max_carryover_seconds", None) is None


def test_stuck_run_flagged_end_to_end(tmp_config, tmp_path):
    state_file = tmp_path / "state.json"
    _write(tmp_config, f"""
[app]
state_file = "{state_file}"

[[pipelines]]
name = "etl_daily"
schedule = "@daily"
max_staleness_seconds = 86400
max_carryover_seconds = 300
""")
    state_file.write_text(json.dumps({
        "etl_daily": {
            "started": "2024-06-01T11:00:00+00:00",
            "finished": None,
            "status": "running",
        }
    }))
    app_cfg = load_config(str(tmp_config))
    store = StateStore(str(state_file))
    results = check_all_carryover(app_cfg, store, now_fn=lambda: DT)
    assert len(results) == 1
    assert results[0].exceeded is True
    assert results[0].pipeline == "etl_daily"
