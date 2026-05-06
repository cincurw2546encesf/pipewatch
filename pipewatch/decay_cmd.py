"""CLI commands for decay scoring."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.decay import DecayStore, check_all_decay


def _get_store(ctx: click.Context) -> DecayStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "decay.json"
    return DecayStore(path)


@click.group("decay")
def decay_cmd() -> None:
    """Manage pipeline decay scores."""


@decay_cmd.command("check")
@click.pass_context
def check_decay_cmd(ctx: click.Context) -> None:
    """Print decay scores for all configured pipelines."""
    app_cfg = ctx.obj["app_cfg"]
    store = _get_store(ctx)
    results = check_all_decay(app_cfg.pipelines, store)
    if not results:
        click.echo("No pipelines have decay configuration.")
        return
    any_exceeded = False
    for r in results:
        click.echo(r.summary())
        if r.exceeded:
            any_exceeded = True
    ctx.exit(1 if any_exceeded else 0)


@decay_cmd.command("list")
@click.pass_context
def list_decay(ctx: click.Context) -> None:
    """List raw decay entries from the store."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No decay entries recorded.")
        return
    for name, entry in sorted(entries.items()):
        click.echo(f"{name}: failures={entry.failure_count} last_failure={entry.last_failure or 'never'}")


@decay_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_decay(ctx: click.Context, pipeline: str) -> None:
    """Reset the decay score for PIPELINE."""
    store = _get_store(ctx)
    store.reset(pipeline)
    click.echo(f"Decay score reset for '{pipeline}'.")
