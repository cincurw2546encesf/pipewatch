"""CLI sub-commands for inspecting and managing pipeline cooldown state."""

from __future__ import annotations

from pathlib import Path

import click

from pipewatch.cooldown import CooldownStore


def _get_store(ctx: click.Context) -> CooldownStore:
    cfg = ctx.obj["app_cfg"]
    return CooldownStore(Path(cfg.state_dir) / "cooldowns.json")


@click.group("cooldown")
def cooldown_cmd() -> None:
    """Manage per-pipeline alert cooldown state."""


@cooldown_cmd.command("list")
@click.pass_context
def list_cooldowns(ctx: click.Context) -> None:
    """List all active cooldown entries."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No active cooldowns.")
        return
    for entry in entries:
        remaining = entry.seconds_remaining()
        status = "cooling" if entry.is_cooling() else "expired"
        click.echo(
            f"{entry.pipeline:30s}  alerted={entry.alerted_at.isoformat()}  "
            f"cooldown={entry.cooldown_seconds}s  remaining={remaining:.0f}s  [{status}]"
        )


@cooldown_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_cooldown(ctx: click.Context, pipeline: str) -> None:
    """Remove the cooldown entry for PIPELINE."""
    store = _get_store(ctx)
    removed = store.reset(pipeline)
    if removed:
        click.echo(f"Cooldown reset for '{pipeline}'.")
    else:
        click.echo(f"No cooldown entry found for '{pipeline}'.")
