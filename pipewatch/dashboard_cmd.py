"""CLI command for displaying the terminal dashboard."""
import click
from pipewatch.checker import PipelineChecker
from pipewatch.dashboard import build_dashboard
from pipewatch.state import StateStore


@click.command("dashboard")
@click.pass_context
def dashboard_cmd(ctx: click.Context) -> None:
    """Display a live terminal dashboard of pipeline health."""
    app_cfg = ctx.obj["config"]
    store = StateStore(app_cfg.state_file)
    checker = PipelineChecker(store)
    results = [checker.check(p) for p in app_cfg.pipelines]
    output = build_dashboard(results)
    click.echo(output)
    summary_ok = all(
        r.status.name == "OK" for r in results
    )
    ctx.exit(0 if summary_ok else 1)
