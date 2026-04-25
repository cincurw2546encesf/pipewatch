"""CLI commands for inspecting and managing pipeline rate limit state."""
from __future__ import annotations

import datetime
from pathlib import Path

import click

from pipewatch.ratelimit import RateLimitStore


def _get_store(ctx: click.Context) -> RateLimitStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "ratelimit.json"
    return RateLimitStore(path)


@click.group("ratelimit")
def ratelimit_cmd() -> None:
    """Manage pipeline check rate limiting."""


@ratelimit_cmd.command("list")
@click.pass_context
def list_ratelimits(ctx: click.Context) -> None:
    """List all rate-limited pipelines and their last check time."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No rate limit records found.")
        return
    for entry in sorted(entries, key=lambda e: e.pipeline):
        ts = datetime.datetime.utcfromtimestamp(entry.last_checked).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        click.echo(
            f"{entry.pipeline:30s}  last_checked={ts} UTC  checks={entry.check_count}"
        )


@ratelimit_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_ratelimit(ctx: click.Context, pipeline: str) -> None:
    """Reset rate limit state for PIPELINE."""
    store = _get_store(ctx)
    store.reset(pipeline)
    click.echo(f"Rate limit state cleared for '{pipeline}'.")
