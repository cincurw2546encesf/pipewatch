"""CLI commands for managing alert suppressions."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import click

from pipewatch.suppression import SuppressionStore


def _get_store(ctx: click.Context) -> SuppressionStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "suppressions.json"
    return SuppressionStore(path)


@click.group("suppression")
def suppression_cmd() -> None:
    """Manage alert suppressions."""


@suppression_cmd.command("add")
@click.argument("pipeline")
@click.option("--minutes", default=60, show_default=True, help="Duration in minutes.")
@click.option("--reason", default="", help="Reason for suppression.")
@click.pass_context
def add_suppression(ctx: click.Context, pipeline: str, minutes: int, reason: str) -> None:
    """Suppress alerts for PIPELINE for a given number of minutes."""
    store = _get_store(ctx)
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    entry = store.suppress(pipeline, reason=reason, until=until)
    click.echo(
        f"Suppressed '{pipeline}' until {entry.suppressed_until.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        + (f" — {reason}" if reason else "")
    )


@suppression_cmd.command("remove")
@click.argument("pipeline")
@click.pass_context
def remove_suppression(ctx: click.Context, pipeline: str) -> None:
    """Remove suppression for PIPELINE."""
    store = _get_store(ctx)
    if store.remove(pipeline):
        click.echo(f"Suppression removed for '{pipeline}'.")
    else:
        click.echo(f"No active suppression found for '{pipeline}'.")


@suppression_cmd.command("list")
@click.pass_context
def list_suppressions(ctx: click.Context) -> None:
    """List all active suppressions."""
    store = _get_store(ctx)
    active = store.all_active()
    if not active:
        click.echo("No active suppressions.")
        return
    for entry in active:
        until_str = entry.suppressed_until.strftime("%Y-%m-%d %H:%M:%S")
        reason_str = f" — {entry.reason}" if entry.reason else ""
        click.echo(f"  {entry.pipeline:<30} until {until_str} UTC{reason_str}")


@suppression_cmd.command("purge")
@click.pass_context
def purge_suppressions(ctx: click.Context) -> None:
    """Remove all expired suppressions."""
    store = _get_store(ctx)
    removed = store.purge_expired()
    click.echo(f"Purged {removed} expired suppression(s).")
