"""CLI commands for managing pipeline silences."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import click

from pipewatch.silencer import SilenceStore


def _get_store(ctx: click.Context) -> SilenceStore:
    state_dir = Path(ctx.obj.get("state_dir", "."))
    return SilenceStore(state_dir / "silences.json")


@click.group("silence")
def silence_cmd():
    """Manage alert silences for pipelines."""


@silence_cmd.command("add")
@click.argument("pipeline")
@click.option("--hours", default=1, show_default=True, help="Duration in hours.")
@click.option("--reason", default="", help="Reason for silencing.")
@click.pass_context
def add_silence(ctx, pipeline: str, hours: int, reason: str):
    """Silence alerts for PIPELINE for a given number of hours."""
    store = _get_store(ctx)
    until = datetime.now(timezone.utc) + timedelta(hours=hours)
    entry = store.silence(pipeline, until, reason=reason)
    click.echo(f"Silenced '{pipeline}' until {entry.until} (reason: {reason or 'none'})")


@silence_cmd.command("remove")
@click.argument("pipeline")
@click.pass_context
def remove_silence(ctx, pipeline: str):
    """Remove an active silence for PIPELINE."""
    store = _get_store(ctx)
    if store.unsilence(pipeline):
        click.echo(f"Removed silence for '{pipeline}'.")
    else:
        click.echo(f"No active silence found for '{pipeline}'.")


@silence_cmd.command("list")
@click.pass_context
def list_silences(ctx):
    """List all currently active silences."""
    store = _get_store(ctx)
    entries = store.active_entries()
    if not entries:
        click.echo("No active silences.")
        return
    for e in entries:
        click.echo(f"  {e.pipeline:30s}  until {e.until}  reason: {e.reason or 'none'}")


@silence_cmd.command("prune")
@click.pass_context
def prune_silences(ctx):
    """Remove expired silence entries from storage."""
    store = _get_store(ctx)
    n = store.prune()
    click.echo(f"Pruned {n} expired silence(s).")
