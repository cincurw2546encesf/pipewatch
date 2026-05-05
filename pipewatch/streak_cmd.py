"""CLI commands for pipeline streak tracking."""
from pathlib import Path

import click

from pipewatch.streak import StreakStore


def _get_store(ctx: click.Context) -> StreakStore:
    cfg = ctx.obj["app_cfg"]
    return StreakStore(Path(cfg.state_dir) / "streaks.json")


@click.group("streak")
def streak_cmd() -> None:
    """Manage pipeline run streaks."""


@streak_cmd.command("list")
@click.pass_context
def list_streaks(ctx: click.Context) -> None:
    """List current streaks for all tracked pipelines."""
    store = _get_store(ctx)
    entries = store.all()
    if not entries:
        click.echo("No streak data recorded.")
        return
    for entry in sorted(entries, key=lambda e: e.pipeline):
        icon = "✅" if entry.current_status == "ok" else "❌"
        click.echo(
            f"{icon}  {entry.pipeline:30s}  "
            f"{entry.current_status:4s}  x{entry.count}  "
            f"since {entry.started_at}"
        )


@streak_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_streak(ctx: click.Context, pipeline: str) -> None:
    """Reset the streak record for PIPELINE."""
    store = _get_store(ctx)
    if store.get(pipeline) is None:
        click.echo(f"No streak entry found for '{pipeline}'.")
        return
    store._data.pop(pipeline, None)
    store._save()
    click.echo(f"Streak reset for '{pipeline}'.")
