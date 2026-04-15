"""CLI helpers for the `pipewatch history` sub-command."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import click

from pipewatch.history import HistoryStore

_DEFAULT_PATH = Path(".pipewatch_history.json")


@click.group(name="history")
def history_cmd() -> None:
    """Manage pipeline run history."""


@history_cmd.command(name="list")
@click.option("--pipeline", "-p", default=None, help="Filter by pipeline name.")
@click.option(
    "--history-file",
    default=str(_DEFAULT_PATH),
    show_default=True,
    help="Path to the history JSON file.",
)
def list_history(pipeline: Optional[str], history_file: str) -> None:
    """List recorded history entries."""
    store = HistoryStore(path=Path(history_file))
    entries = store.for_pipeline(pipeline) if pipeline else store.all_entries()

    if not entries:
        click.echo("No history entries found.")
        return

    for entry in entries:
        last = entry.last_run or "—"
        click.echo(
            f"[{entry.checked_at}] {entry.pipeline:30s}  {entry.status:8s}  last_run={last}"
        )


@history_cmd.command(name="clear")
@click.option("--pipeline", "-p", default=None, help="Clear only this pipeline's history.")
@click.option(
    "--history-file",
    default=str(_DEFAULT_PATH),
    show_default=True,
    help="Path to the history JSON file.",
)
@click.confirmation_option(prompt="Are you sure you want to clear history?")
def clear_history(pipeline: Optional[str], history_file: str) -> None:
    """Remove history entries (all or for one pipeline)."""
    store = HistoryStore(path=Path(history_file))
    removed = store.clear(pipeline=pipeline)
    scope = f"pipeline '{pipeline}'" if pipeline else "all pipelines"
    click.echo(f"Removed {removed} history entries for {scope}.")
