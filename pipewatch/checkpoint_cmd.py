"""CLI commands for pipeline checkpoint management."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.checkpoint import CheckpointStore


def _get_store(ctx: click.Context) -> CheckpointStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "checkpoints.json"
    return CheckpointStore(path)


@click.group("checkpoint")
def checkpoint_cmd() -> None:
    """Manage pipeline checkpoints."""


@checkpoint_cmd.command("record")
@click.argument("pipeline")
@click.argument("name")
@click.option("--meta", multiple=True, metavar="KEY=VALUE", help="Optional metadata pairs.")
@click.pass_context
def record_checkpoint(ctx: click.Context, pipeline: str, name: str, meta: tuple) -> None:
    """Record a named checkpoint for a pipeline."""
    store = _get_store(ctx)
    metadata = {}
    for item in meta:
        if "=" in item:
            k, v = item.split("=", 1)
            metadata[k.strip()] = v.strip()
    entry = store.record(pipeline, name, metadata)
    click.echo(f"Recorded: {entry.summary()}")


@checkpoint_cmd.command("list")
@click.argument("pipeline")
@click.pass_context
def list_checkpoints(ctx: click.Context, pipeline: str) -> None:
    """List all checkpoints for a pipeline."""
    store = _get_store(ctx)
    entries = store.get(pipeline)
    if not entries:
        click.echo(f"No checkpoints recorded for '{pipeline}'.")
        return
    for e in entries:
        click.echo(e.summary())
        if e.metadata:
            for k, v in e.metadata.items():
                click.echo(f"  {k}: {v}")


@checkpoint_cmd.command("latest")
@click.argument("pipeline")
@click.pass_context
def latest_checkpoint(ctx: click.Context, pipeline: str) -> None:
    """Show the most recent checkpoint for a pipeline."""
    store = _get_store(ctx)
    entry = store.latest(pipeline)
    if entry is None:
        click.echo(f"No checkpoints recorded for '{pipeline}'.")
        return
    click.echo(entry.summary())


@checkpoint_cmd.command("clear")
@click.argument("pipeline")
@click.pass_context
def clear_checkpoints(ctx: click.Context, pipeline: str) -> None:
    """Remove all checkpoints for a pipeline."""
    store = _get_store(ctx)
    removed = store.clear(pipeline)
    click.echo(f"Removed {removed} checkpoint(s) for '{pipeline}'.")
