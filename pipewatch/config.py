"""Configuration loader for pipewatch pipelines."""

import os
import yaml
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PipelineConfig:
    name: str
    schedule: str  # cron expression or interval like '1h', '30m'
    max_age_minutes: int = 60
    alert_email: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class AppConfig:
    pipelines: List[PipelineConfig] = field(default_factory=list)
    state_file: str = ".pipewatch_state.json"
    log_level: str = "INFO"


def load_config(path: str = "pipewatch.yml") -> AppConfig:
    """Load and parse the pipewatch YAML configuration file."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Config file not found: {path}. "
            "Run `pipewatch init` to create a default config."
        )

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if raw is None:
        raise ValueError(f"Config file is empty: {path}")

    pipelines = []
    for entry in raw.get("pipelines", []):
        pipelines.append(
            PipelineConfig(
                name=entry["name"],
                schedule=entry["schedule"],
                max_age_minutes=entry.get("max_age_minutes", 60),
                alert_email=entry.get("alert_email"),
                tags=entry.get("tags", []),
                enabled=entry.get("enabled", True),
            )
        )

    return AppConfig(
        pipelines=pipelines,
        state_file=raw.get("state_file", ".pipewatch_state.json"),
        log_level=raw.get("log_level", "INFO"),
    )


DEFAULT_CONFIG_TEMPLATE = """\
log_level: INFO
state_file: .pipewatch_state.json

pipelines:
  - name: example_pipeline
    schedule: "0 * * * *"  # every hour
    max_age_minutes: 90
    alert_email: null
    tags:
      - etl
    enabled: true
"""
