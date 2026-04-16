"""CLI commands for inspecting retry state."""
from pathlib import Path
import click
from pipewatch.retry import RetryStore


def _get_store(ctx: click.Context) -> RetryStore:
    cfg = ctx.obj.get("app_cfg")
    state_dir = Path(cfg.state_dir) if cfg else Path(".pipewatch")
    return RetryStore(state_dir / "retries.json")


@click.group("retry")
def retry_cmd() -> None:
    """Manage pipeline retry state."""


@retry_cmd.command("list")
@click.pass_context
def list_retries(ctx: click.Context) -> None:
    """List all pipelines with recorded retry attempts."""
    store = _get_store(ctx)
    entries = store.all_entries()
    if not entries:
        click.echo("No retry records found.")
        return
    for e in entries:
        la = e.last_attempt.strftime("%Y-%m-%d %H:%M:%S UTC") if e.last_attempt else "never"
        err = e.last_error or "-"
        click.echo(f"{e.pipeline}: {e.attempts} attempt(s), last={la}, error={err}")


@retry_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_retry(ctx: click.Context, pipeline: str) -> None:
    """Reset retry counter for a pipeline."""
    store = _get_store(ctx)
    store.reset(pipeline)
    click.echo(f"Retry state cleared for '{pipeline}'.")
