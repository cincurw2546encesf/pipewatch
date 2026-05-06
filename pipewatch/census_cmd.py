"""CLI commands for the census feature."""
from __future__ import annotations

import click

from pipewatch.history import HistoryStore
from pipewatch.census import check_all_census


@click.group("census")
def census_cmd() -> None:
    """Count pipeline run statuses over a rolling time window."""


@census_cmd.command("check")
@click.option("--window", default=86400, show_default=True, help="Window in seconds.")
@click.pass_context
def check_census_cmd(ctx: click.Context, window: int) -> None:
    """Print a status census for every pipeline."""
    app_cfg = ctx.obj["config"]
    store = HistoryStore(app_cfg.history_file)
    pipelines = [p.name for p in app_cfg.pipelines]

    if not pipelines:
        click.echo("No pipelines configured.")
        return

    results = check_all_census(pipelines, window, store)
    for r in results:
        click.echo(r.summary)
