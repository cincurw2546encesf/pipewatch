"""Render annotation summaries alongside check results."""
from __future__ import annotations
from pipewatch.annotation import AnnotationStore
from pipewatch.checker import CheckResult


def annotated_text_report(results: list[CheckResult], store: AnnotationStore) -> str:
    lines: list[str] = []
    for r in results:
        lines.append(f"{r.pipeline}: {r.status.value}")
        notes = store.get(r.pipeline)
        if notes:
            for ann in notes[-3:]:  # show up to last 3
                ts = ann.created_at.strftime("%Y-%m-%d %H:%M")
                lines.append(f"  [{ts}] {ann.author}: {ann.note}")
    return "\n".join(lines)


def pipelines_with_annotations(results: list[CheckResult], store: AnnotationStore) -> list[str]:
    """Return pipeline names that have at least one annotation."""
    return [r.pipeline for r in results if store.get(r.pipeline)]
