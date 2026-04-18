"""CLI commands for pipeline snapshots."""
from __future__ import annotations

import click

from pipewatch.snapshot import load_snapshot, take_snapshot


@click.group("snapshot")
def snapshot_cmd():
    """Save and inspect point-in-time pipeline snapshots."""


@snapshot_cmd.command("show")
@click.argument("path", default=".pipewatch/snapshot.json")
def show_snapshot(path: str):
    """Display the latest saved snapshot."""
    snap = load_snapshot(path)
    if snap is None:
        click.echo("No snapshot found.")
        return
    click.echo(f"Snapshot taken at: {snap.taken_at}")
    click.echo(f"{'Pipeline':<30} {'Status':<10} Last Run")
    click.echo("-" * 65)
    for e in snap.entries:
        last = e.last_run or "never"
        click.echo(f"{e.pipeline:<30} {e.status:<10} {last}")


@snapshot_cmd.command("save")
@click.argument("path", default=".pipewatch/snapshot.json")
@click.pass_context
def save_snapshot(ctx: click.Context, path: str):
    """Run checks and save a new snapshot."""
    obj = ctx.obj or {}
    results = obj.get("results", [])
    if not results:
        click.echo("No results available; run 'check' first or pass results via context.")
        return
    snap = take_snapshot(results, path)
    click.echo(f"Snapshot saved to {path} ({len(snap.entries)} pipelines, {snap.taken_at})")
