"""Tests verifying that tags are parsed from config and exposed correctly."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipewatch.config import load_config


@pytest.fixture()
def tmp_config(tmp_path: Path):
    """Write a YAML config and return its path."""
    def _write(content: str) -> Path:
        p = tmp_path / "pipewatch.yaml"
        p.write_text(textwrap.dedent(content))
        return p
    return _write


def test_tags_parsed_correctly(tmp_config):
    p = tmp_config("""
        pipelines:
          - name: finance-daily
            cron: "0 6 * * *"
            tags: [finance, daily]
          - name: marketing-hourly
            cron: "0 * * * *"
            tags: [marketing, hourly]
    """)
    cfg = load_config(p)
    assert cfg.pipelines[0].tags == ["finance", "daily"]
    assert cfg.pipelines[1].tags == ["marketing", "hourly"]


def test_tags_default_empty(tmp_config):
    p = tmp_config("""
        pipelines:
          - name: no-tags
            cron: "0 0 * * *"
    """)
    cfg = load_config(p)
    assert cfg.pipelines[0].tags == []


def test_tags_null_treated_as_empty(tmp_config):
    p = tmp_config("""
        pipelines:
          - name: null-tags
            cron: "0 0 * * *"
            tags: null
    """)
    cfg = load_config(p)
    assert cfg.pipelines[0].tags == []


def test_multiple_pipelines_independent_tags(tmp_config):
    p = tmp_config("""
        pipelines:
          - name: a
            cron: "0 1 * * *"
            tags: [alpha]
          - name: b
            cron: "0 2 * * *"
            tags: [beta, gamma]
          - name: c
            cron: "0 3 * * *"
    """)
    cfg = load_config(p)
    assert cfg.pipelines[0].tags == ["alpha"]
    assert cfg.pipelines[1].tags == ["beta", "gamma"]
    assert cfg.pipelines[2].tags == []
