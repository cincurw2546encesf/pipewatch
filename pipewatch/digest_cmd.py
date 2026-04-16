"""CLI command for generating a digest report."""
import json
import click

from pipewatch.checker import PipelineChecker
from pipewatch.digest import build_digest, format_digest_text
from pipewatch.history import HistoryStore
from pipewatch.trend import analyse_trend


@click.group("digest")
def digest_cmd():
    """Digest report commands."""


@digest_cmd.command("show")
@click.option("--format", "fmt", type=click.Choice(["text", "json"]), default="text")
@click.pass_context
def show_digest(ctx, fmt):
    """Print a digest summary of all pipeline statuses."""
    app_cfg = ctx.obj["config"]
    state_store = ctx.obj["store"]
    history_path = ctx.obj.get("history_path", "pipewatch_history.json")

    checker = PipelineChecker(app_cfg, state_store)
    results = checker.check_all()

    history_store = HistoryStore(history_path)
    trends = {}
    for r in results:
        entries = history_store.get(r.pipeline)
        if entries:
            trends[r.pipeline] = analyse_trend(entries)

    report = build_digest(results, trends)

    if fmt == "json":
        data = {
            "generated_at": report.generated_at,
            "total": report.total,
            "healthy": report.healthy,
            "stale": report.stale,
            "failed": report.failed,
            "entries": [
                {
                    "pipeline": e.pipeline,
                    "status": e.status.value,
                    "last_run": e.last_run,
                    "failure_rate": e.failure_rate,
                    "trend": e.trend,
                }
                for e in report.entries
            ],
        }
        click.echo(json.dumps(data, indent=2))
    else:
        click.echo(format_digest_text(report))
