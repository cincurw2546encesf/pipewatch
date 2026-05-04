"""CLI commands for managing pipeline grace periods."""

from __future__ import annotations

from datetime import timezone
from pathlib import Path

import click

from pipewatch.grace import GraceStore


def _get_store(ctx: click.Context) -> GraceStore:
    cfg = ctx.obj["app_cfg"]
    return GraceStore(Path(cfg.state_dir) / "grace.json")


@click.group("grace")
def grace_cmd() -> None:
    """Manage pipeline grace periods."""


@grace_cmd.command("add")
@click.argument("pipeline")
@click.argument("seconds", type=int)
@click.pass_context
def add_grace(ctx: click.Context, pipeline: str, seconds: int) -> None:
    """Register a grace period for PIPELINE lasting SECONDS seconds."""
    store = _get_store(ctx)
    entry = store.register(pipeline, seconds)
    registered = entry.registered_at.astimezone(timezone.utc).isoformat()
    click.echo(f"Grace period registered for '{pipeline}': {seconds}s from {registered}")


@grace_cmd.command("remove")
@click.argument("pipeline")
@click.pass_context
def remove_grace(ctx: click.Context, pipeline: str) -> None:
    """Remove the grace period for PIPELINE."""
    store = _get_store(ctx)
    removed = store.remove(pipeline)
    if removed:
        click.echo(f"Grace period removed for '{pipeline}'.")
    else:
        click.echo(f"No grace period found for '{pipeline}'.")


@grace_cmd.command("list")
@click.pass_context
def list_graces(ctx: click.Context) -> None:
    """List all registered grace periods."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No grace periods registered.")
        return
    for e in entries:
        remaining = e.seconds_remaining()
        status = "active" if e.is_active() else "expired"
        click.echo(f"  {e.pipeline}: {e.grace_seconds}s total, {remaining:.0f}s remaining [{status}]")
