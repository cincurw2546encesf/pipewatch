"""CLI commands for inspecting and managing alert throttle state."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.throttle import ThrottleStore


def _get_store(ctx: click.Context) -> ThrottleStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "throttle.json"
    return ThrottleStore(path)


@click.group("throttle")
def throttle_cmd() -> None:
    """Manage alert throttle state."""


@throttle_cmd.command("list")
@click.pass_context
def list_throttles(ctx: click.Context) -> None:
    """List pipelines currently under alert throttle."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No throttled pipelines.")
        return
    click.echo(f"{'Pipeline':<30} {'Last Alerted':<28} {'Count':>5}")
    click.echo("-" * 66)
    for e in sorted(entries, key=lambda x: x.pipeline):
        click.echo(
            f"{e.pipeline:<30} {e.last_alerted.isoformat():<28} {e.alert_count:>5}"
        )


@throttle_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_throttle(ctx: click.Context, pipeline: str) -> None:
    """Reset throttle state for PIPELINE."""
    store = _get_store(ctx)
    if store.get(pipeline) is None:
        click.echo(f"No throttle entry found for '{pipeline}'.")
        return
    store.reset(pipeline)
    click.echo(f"Throttle state cleared for '{pipeline}'.")
