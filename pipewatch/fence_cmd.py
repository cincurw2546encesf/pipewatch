"""fence_cmd.py — CLI commands for managing pipeline concurrency fences."""
from __future__ import annotations

import os
import socket

import click

from pipewatch.fence import FenceStore


def _get_store(ctx: click.Context) -> FenceStore:
    cfg = ctx.obj["app_cfg"]
    path = os.path.join(cfg.state_dir, "fence.json")
    return FenceStore(path)


@click.group("fence")
def fence_cmd() -> None:
    """Manage concurrency fences for pipelines."""


@fence_cmd.command("acquire")
@click.argument("pipeline")
@click.option("--owner", default=None, help="Owner identifier (default: hostname)")
@click.pass_context
def acquire_fence(ctx: click.Context, pipeline: str, owner: str | None) -> None:
    """Acquire a concurrency fence for PIPELINE."""
    store = _get_store(ctx)
    resolved_owner = owner or socket.gethostname()
    acquired = store.acquire(pipeline, resolved_owner)
    if acquired:
        click.echo(f"Fence acquired for '{pipeline}' by {resolved_owner}.")
    else:
        result = store.check(pipeline)
        click.echo(
            f"Could not acquire fence for '{pipeline}': "
            f"already locked by {result.owner}.",
            err=True,
        )
        ctx.exit(1)


@fence_cmd.command("release")
@click.argument("pipeline")
@click.pass_context
def release_fence(ctx: click.Context, pipeline: str) -> None:
    """Release the concurrency fence for PIPELINE."""
    store = _get_store(ctx)
    released = store.release(pipeline)
    if released:
        click.echo(f"Fence released for '{pipeline}'.")
    else:
        click.echo(f"No active fence found for '{pipeline}'.")


@fence_cmd.command("list")
@click.pass_context
def list_fences(ctx: click.Context) -> None:
    """List all active concurrency fences."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No active fences.")
        return
    for entry in entries:
        click.echo(
            f"  {entry.pipeline:30s}  owner={entry.owner}  "
            f"since={entry.locked_at.isoformat()}"
        )
