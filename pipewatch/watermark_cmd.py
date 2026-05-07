"""CLI commands for watermark management."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import click

from pipewatch.watermark import WatermarkStore


def _get_store(ctx: click.Context) -> WatermarkStore:
    cfg = ctx.obj["app_cfg"]
    return WatermarkStore(Path(cfg.state_dir) / "watermarks.json")


@click.group("watermark")
def watermark_cmd() -> None:
    """Manage pipeline watermarks."""


@watermark_cmd.command("list")
@click.pass_context
def list_watermarks(ctx: click.Context) -> None:
    """List all recorded watermarks."""
    store = _get_store(ctx)
    entries = store.all()
    if not entries:
        click.echo("No watermarks recorded.")
        return
    for entry in sorted(entries, key=lambda e: e.pipeline):
        click.echo(f"  {entry.pipeline}: {entry.high_water.isoformat()}")


@watermark_cmd.command("update")
@click.argument("pipeline")
@click.argument("timestamp")
@click.pass_context
def update_watermark(ctx: click.Context, pipeline: str, timestamp: str) -> None:
    """Update the watermark for PIPELINE to TIMESTAMP (ISO 8601)."""
    store = _get_store(ctx)
    try:
        ts = datetime.fromisoformat(timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except ValueError:
        click.echo(f"Invalid timestamp: {timestamp}", err=True)
        raise SystemExit(1)
    result = store.update(pipeline, ts)
    click.echo(result.summary())
    if result.regressed:
        raise SystemExit(2)


@watermark_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_watermark(ctx: click.Context, pipeline: str) -> None:
    """Remove the watermark entry for PIPELINE."""
    store = _get_store(ctx)
    removed = store.reset(pipeline)
    if removed:
        click.echo(f"Watermark for '{pipeline}' removed.")
    else:
        click.echo(f"No watermark found for '{pipeline}'.")
