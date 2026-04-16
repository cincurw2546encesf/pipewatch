"""CLI commands for managing pipeline annotations."""
from pathlib import Path
import click
from pipewatch.annotation import AnnotationStore


def _get_store(ctx: click.Context) -> AnnotationStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "annotations.json"
    return AnnotationStore(path)


@click.group("annotation")
def annotation_cmd() -> None:
    """Manage pipeline run annotations."""


@annotation_cmd.command("add")
@click.argument("pipeline")
@click.argument("note")
@click.option("--author", default="cli", show_default=True)
@click.pass_context
def add_annotation(ctx: click.Context, pipeline: str, note: str, author: str) -> None:
    """Add an annotation to a pipeline."""
    store = _get_store(ctx)
    ann = store.add(pipeline, note, author)
    click.echo(f"Annotation added to '{pipeline}' by {ann.author} at {ann.created_at.isoformat()}")


@annotation_cmd.command("list")
@click.argument("pipeline")
@click.pass_context
def list_annotations(ctx: click.Context, pipeline: str) -> None:
    """List annotations for a pipeline."""
    store = _get_store(ctx)
    entries = store.get(pipeline)
    if not entries:
        click.echo(f"No annotations for '{pipeline}'.")
        return
    for ann in entries:
        click.echo(f"[{ann.created_at.isoformat()}] {ann.author}: {ann.note}")


@annotation_cmd.command("clear")
@click.argument("pipeline")
@click.pass_context
def clear_annotations(ctx: click.Context, pipeline: str) -> None:
    """Remove all annotations for a pipeline."""
    store = _get_store(ctx)
    removed = store.clear(pipeline)
    click.echo(f"Removed {removed} annotation(s) for '{pipeline}'.")
