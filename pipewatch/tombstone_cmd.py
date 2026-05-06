"""CLI commands for managing pipeline tombstones."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.tombstone import TombstoneStore


def _get_store(ctx: click.Context) -> TombstoneStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "tombstones.json"
    return TombstoneStore(path)


@click.group("tombstone")
def tombstone_cmd() -> None:
    """Manage retired/decommissioned pipelines."""


@tombstone_cmd.command("retire")
@click.argument("pipeline")
@click.option("--reason", required=True, help="Why this pipeline is being retired.")
@click.option("--by", "retired_by", default=None, help="Who is retiring the pipeline.")
@click.pass_context
def retire_pipeline(ctx: click.Context, pipeline: str, reason: str, retired_by: str | None) -> None:
    """Mark a pipeline as permanently retired."""
    store = _get_store(ctx)
    entry = store.retire(pipeline, reason=reason, retired_by=retired_by)
    click.echo(f"Retired: {entry.summary()}")


@tombstone_cmd.command("restore")
@click.argument("pipeline")
@click.pass_context
def restore_pipeline(ctx: click.Context, pipeline: str) -> None:
    """Remove a tombstone, restoring a pipeline to active monitoring."""
    store = _get_store(ctx)
    if store.restore(pipeline):
        click.echo(f"Restored: {pipeline} is no longer retired.")
    else:
        click.echo(f"No tombstone found for '{pipeline}'.")


@tombstone_cmd.command("list")
@click.pass_context
def list_tombstones(ctx: click.Context) -> None:
    """List all retired pipelines."""
    store = _get_store(ctx)
    entries = store.all()
    if not entries:
        click.echo("No retired pipelines.")
        return
    for entry in sorted(entries, key=lambda e: e.retired_at):
        click.echo(f"  {entry.summary()}")
