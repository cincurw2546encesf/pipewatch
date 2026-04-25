"""CLI commands for heartbeat management."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.heartbeat import HeartbeatStore


def _get_store(ctx: click.Context) -> HeartbeatStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "heartbeats.json"
    return HeartbeatStore(path)


@click.group("heartbeat")
def heartbeat_cmd() -> None:
    """Manage pipeline heartbeats."""


@heartbeat_cmd.command("beat")
@click.argument("pipeline")
@click.option("--interval", default=300, show_default=True, help="Expected max seconds between beats.")
@click.pass_context
def record_beat(ctx: click.Context, pipeline: str, interval: int) -> None:
    """Record a heartbeat ping for PIPELINE."""
    store = _get_store(ctx)
    entry = store.beat(pipeline, interval_seconds=interval)
    click.echo(f"Heartbeat recorded for '{pipeline}' (interval {interval}s) at {entry.last_beat.isoformat()}.")


@heartbeat_cmd.command("check")
@click.argument("pipeline")
@click.pass_context
def check_beat(ctx: click.Context, pipeline: str) -> None:
    """Check heartbeat status for PIPELINE."""
    store = _get_store(ctx)
    report = store.check(pipeline)
    if report is None:
        click.echo(f"No heartbeat registered for '{pipeline}'.")
        raise SystemExit(1)
    click.echo(str(report))
    if not report.alive:
        raise SystemExit(1)


@heartbeat_cmd.command("list")
@click.pass_context
def list_beats(ctx: click.Context) -> None:
    """List all registered heartbeats and their current status."""
    store = _get_store(ctx)
    reports = store.check_all()
    if not reports:
        click.echo("No heartbeats registered.")
        return
    for r in reports:
        click.echo(str(r))


@heartbeat_cmd.command("remove")
@click.argument("pipeline")
@click.pass_context
def remove_beat(ctx: click.Context, pipeline: str) -> None:
    """Remove heartbeat tracking for PIPELINE."""
    store = _get_store(ctx)
    if store.remove(pipeline):
        click.echo(f"Heartbeat entry for '{pipeline}' removed.")
    else:
        click.echo(f"No heartbeat entry found for '{pipeline}'.")
        raise SystemExit(1)
