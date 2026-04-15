"""Configuration models and loader for pipewatch."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class PipelineConfig:
    name: str
    cron: str
    max_stale_minutes: int = 60
    tags: List[str] = field(default_factory=list)
    alert_on_stale: bool = True
    alert_on_failure: bool = True


@dataclass
class AppConfig:
    state_dir: str = ".pipewatch"
    check_interval_seconds: int = 60
    pipelines: List[PipelineConfig] = field(default_factory=list)


def _parse_pipeline(raw: Dict[str, Any]) -> PipelineConfig:
    return PipelineConfig(
        name=raw["name"],
        cron=raw["cron"],
        max_stale_minutes=int(raw.get("max_stale_minutes", 60)),
        tags=list(raw.get("tags") or []),
        alert_on_stale=bool(raw.get("alert_on_stale", True)),
        alert_on_failure=bool(raw.get("alert_on_failure", True)),
    )


def load_config(path: str | os.PathLike = "pipewatch.yaml") -> AppConfig:
    """Load and parse the YAML configuration file."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as fh:
        raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    pipelines = [_parse_pipeline(p) for p in raw.get("pipelines", [])]

    return AppConfig(
        state_dir=raw.get("state_dir", ".pipewatch"),
        check_interval_seconds=int(raw.get("check_interval_seconds", 60)),
        pipelines=pipelines,
    )
