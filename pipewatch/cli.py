"""PipeWatch CLI — check pipelines, list status, and generate reports."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click

from pipewatch.checker import check_pipeline
from pipewatch.config import AppConfig, load_config
from pipewatch.reporter import ReportFormat, build_report
from pipewatch.state import StateStore

DEFAULT_CONFIG = "pipewatch.yaml"


@click.group()
@click.option("--config", default=DEFAULT_CONFIG, show_default=True, help="Path to config file.")
@click.pass_context
def cli(ctx: click.Context, config: str) -> None:
    """PipeWatch — ETL pipeline health monitor."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


@cli.command(name="check")
@click.pass_context
def check_cmd(ctx: click.Context) -> None:
    """Run health checks on all configured pipelines."""
    cfg: AppConfig = load_config(ctx.obj["config_path"])
    store = StateStore(cfg.state_file)
    results = [check_pipeline(p, store) for p in cfg.pipelines]
    for r in results:
        click.echo(str(r))
    failed = any(r.status.value != "ok" for r in results)
    sys.exit(1 if failed else 0)


@cli.command(name="list")
@click.pass_context
def list_cmd(ctx: click.Context) -> None:
    """List all configured pipelines."""
    cfg: AppConfig = load_config(ctx.obj["config_path"])
    for p in cfg.pipelines:
        click.echo(
            f"  {p.name:<28} max_age={p.max_age_minutes}m  "
            f"on_failure={p.alert_on_failure}"
        )


@cli.command(name="report")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json", "csv"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option("--output", "-o", default=None, help="Write report to file instead of stdout.")
@click.pass_context
def report_cmd(ctx: click.Context, fmt: ReportFormat, output: Optional[str]) -> None:
    """Generate a summary report of pipeline health."""
    cfg: AppConfig = load_config(ctx.obj["config_path"])
    store = StateStore(cfg.state_file)
    results = [check_pipeline(p, store) for p in cfg.pipelines]
    report = build_report(results, fmt=fmt)
    if output:
        Path(output).write_text(report, encoding="utf-8")
        click.echo(f"Report written to {output}")
    else:
        click.echo(report)


def main() -> None:  # pragma: no cover
    cli(obj={})


if __name__ == "__main__":  # pragma: no cover
    main()
