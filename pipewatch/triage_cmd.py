import click
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.triage import TriageEntry, TriageReport, build_triage_report


def _get_app_cfg(ctx):
    return ctx.obj["app_cfg"]


def _get_results(ctx):
    return ctx.obj.get("results", [])


@click.group(name="triage")
def triage_cmd():
    """Triage and prioritise pipeline issues."""


@triage_cmd.command(name="show")
@click.option("--status", type=click.Choice(["stale", "failed", "ok"]), default=None, help="Filter by status")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output as JSON")
@click.pass_context
def show_triage(ctx, status, as_json):
    """Show triage report for all pipelines."""
    results = _get_results(ctx)
    report = build_triage_report(results)

    entries = report.entries
    if status:
        entries = [e for e in entries if e.status.lower() == status]

    if not entries:
        click.echo("No issues found.")
        return

    if as_json:
        import json
        click.echo(json.dumps([{
            "pipeline": e.pipeline,
            "status": e.status,
            "summary": e.summary,
            "last_run": e.last_run,
            "priority": e.priority,
        } for e in entries], indent=2))
        return

    click.echo(f"{'Pipeline':<30} {'Status':<10} {'Priority':<10} Summary")
    click.echo("-" * 80)
    for e in entries:
        click.echo(f"{e.pipeline:<30} {e.status:<10} {e.priority:<10} {e.summary}")


@triage_cmd.command(name="summary")
@click.pass_context
def summary_triage(ctx):
    """Print a one-line triage summary."""
    results = _get_results(ctx)
    report = build_triage_report(results)
    click.echo(report.summary())
