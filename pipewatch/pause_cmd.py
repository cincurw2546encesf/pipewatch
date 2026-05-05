"""CLI commands for pausing and resuming pipelines."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.pause import PauseStore


def _get_store(ctx: click.Context) -> PauseStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "pauses.json"
    return PauseStore(path)


@click.group(name="pause")
def pause_cmd() -> None:
    """Pause or resume pipeline checks."""


@pause_cmd.command(name="add")
@click.argument("pipeline")
@click.option("--reason", default=None, help="Reason for pausing.")
@click.pass_context
def add_pause(ctx: click.Context, pipeline: str, reason: str | None) -> None:
    """Pause a pipeline."""
    store = _get_store(ctx)
    entry = store.pause(pipeline, reason=reason)
    msg = f"Paused '{pipeline}' at {entry.paused_at.isoformat()}"
    if reason:
        msg += f" — {reason}"
    click.echo(msg)


@pause_cmd.command(name="remove")
@click.argument("pipeline")
@click.pass_context
def remove_pause(ctx: click.Context, pipeline: str) -> None:
    """Resume a paused pipeline."""
    store = _get_store(ctx)
    if store.resume(pipeline):
        click.echo(f"Resumed '{pipeline}'.")
    else:
        click.echo(f"'{pipeline}' is not paused.")


@pause_cmd.command(name="list")
@click.pass_context
def list_pauses(ctx: click.Context) -> None:
    """List all paused pipelines."""
    store = _get_store(ctx)
    entries = store.all_paused()
    if not entries:
        click.echo("No pipelines are currently paused.")
        return
    for e in entries:
        reason_str = f" ({e.reason})" if e.reason else ""
        click.echo(f"{e.pipeline}  paused_at={e.paused_at.isoformat()}{reason_str}")
