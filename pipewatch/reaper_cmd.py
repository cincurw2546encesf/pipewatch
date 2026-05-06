"""CLI commands for the reaper module."""
from __future__ import annotations

import click

from pipewatch.config import AppConfig
from pipewatch.reaper import check_reaper
from pipewatch.state import StateStore


def _get_store(ctx: click.Context) -> StateStore:
    cfg: AppConfig = ctx.obj["config"]
    return StateStore(cfg.state_file)


@click.group("reaper")
def reaper_cmd() -> None:
    """Detect pipelines that have exceeded their expiry window."""


@reaper_cmd.command("check")
@click.argument("pipeline")
@click.option("--expiry", "expiry_seconds", required=True, type=int,
              help="Expiry window in seconds.")
@click.pass_context
def check_reaper_cmd(ctx: click.Context, pipeline: str, expiry_seconds: int) -> None:
    """Check whether PIPELINE has exceeded its expiry window."""
    store = _get_store(ctx)
    result = check_reaper(pipeline, expiry_seconds, store)
    click.echo(result.summary())
    if result.expired:
        ctx.exit(1)


@reaper_cmd.command("list")
@click.pass_context
def list_reaper_cmd(ctx: click.Context) -> None:
    """List all pipelines with an expiry_seconds config and their status."""
    cfg: AppConfig = ctx.obj["config"]
    store = _get_store(ctx)
    found = False
    for pipeline in cfg.pipelines:
        expiry = getattr(pipeline, "expiry_seconds", None)
        if expiry is None:
            continue
        found = True
        result = check_reaper(pipeline.name, expiry, store)
        icon = "✗" if result.expired else "✓"
        click.echo(f"{icon} {result.summary()}")
    if not found:
        click.echo("No pipelines have expiry_seconds configured.")
