"""Exports pipeline check results to various output formats and destinations."""

from __future__ import annotations

import csv
import io
import json
import pathlib
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import CheckResult, CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def export_json(
    results: List[CheckResult],
    path: pathlib.Path,
    *,
    pretty: bool = True,
) -> None:
    """Write results to a JSON file."""
    payload = {
        "exported_at": _utcnow().isoformat(),
        "pipelines": [
            {
                "name": r.pipeline_name,
                "status": r.status.value,
                "message": r.message,
                "last_run": r.last_run.isoformat() if r.last_run else None,
                "age_seconds": r.age_seconds,
            }
            for r in results
        ],
    }
    indent = 2 if pretty else None
    path.write_text(json.dumps(payload, indent=indent), encoding="utf-8")


def export_csv(
    results: List[CheckResult],
    path: pathlib.Path,
) -> None:
    """Write results to a CSV file."""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=["pipeline_name", "status", "message", "last_run", "age_seconds"],
    )
    writer.writeheader()
    for r in results:
        writer.writerow(
            {
                "pipeline_name": r.pipeline_name,
                "status": r.status.value,
                "message": r.message,
                "last_run": r.last_run.isoformat() if r.last_run else "",
                "age_seconds": r.age_seconds if r.age_seconds is not None else "",
            }
        )
    path.write_text(buf.getvalue(), encoding="utf-8")


def export_results(
    results: List[CheckResult],
    path: pathlib.Path,
    fmt: str = "json",
) -> None:
    """Dispatch export to the appropriate format handler.

    Args:
        results: List of check results to export.
        path: Destination file path.
        fmt: One of ``json`` or ``csv``.

    Raises:
        ValueError: If *fmt* is not supported.
    """
    fmt = fmt.lower()
    if fmt == "json":
        export_json(results, path)
    elif fmt == "csv":
        export_csv(results, path)
    else:
        raise ValueError(f"Unsupported export format: {fmt!r}. Choose 'json' or 'csv'.")
