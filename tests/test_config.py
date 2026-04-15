"""Tests for pipewatch configuration loader."""

import os
import pytest
import tempfile

from pipewatch.config import load_config, AppConfig, PipelineConfig, DEFAULT_CONFIG_TEMPLATE


@pytest.fixture
def tmp_config(tmp_path):
    """Write a temporary YAML config and return its path."""
    def _write(content: str) -> str:
        config_file = tmp_path / "pipewatch.yml"
        config_file.write_text(content)
        return str(config_file)
    return _write


def test_load_valid_config(tmp_config):
    path = tmp_config("""
log_level: DEBUG
state_file: /tmp/state.json
pipelines:
  - name: my_pipeline
    schedule: "*/30 * * * *"
    max_age_minutes: 45
    alert_email: ops@example.com
    tags: [etl, critical]
    enabled: true
""")
    config = load_config(path)
    assert isinstance(config, AppConfig)
    assert config.log_level == "DEBUG"
    assert config.state_file == "/tmp/state.json"
    assert len(config.pipelines) == 1

    p = config.pipelines[0]
    assert isinstance(p, PipelineConfig)
    assert p.name == "my_pipeline"
    assert p.schedule == "*/30 * * * *"
    assert p.max_age_minutes == 45
    assert p.alert_email == "ops@example.com"
    assert p.tags == ["etl", "critical"]
    assert p.enabled is True


def test_load_config_defaults(tmp_config):
    path = tmp_config("""
pipelines:
  - name: bare_pipeline
    schedule: "1h"
""")
    config = load_config(path)
    assert config.log_level == "INFO"
    assert config.state_file == ".pipewatch_state.json"
    p = config.pipelines[0]
    assert p.max_age_minutes == 60
    assert p.alert_email is None
    assert p.tags == []
    assert p.enabled is True


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("/nonexistent/path/pipewatch.yml")


def test_load_config_empty_file(tmp_config):
    path = tmp_config("")
    with pytest.raises(ValueError, match="Config file is empty"):
        load_config(path)


def test_load_config_no_pipelines(tmp_config):
    path = tmp_config("log_level: WARNING\n")
    config = load_config(path)
    assert config.pipelines == []
    assert config.log_level == "WARNING"


def test_default_config_template_is_valid(tmp_config):
    path = tmp_config(DEFAULT_CONFIG_TEMPLATE)
    config = load_config(path)
    assert len(config.pipelines) == 1
    assert config.pipelines[0].name == "example_pipeline"
