"""Generate summary reports from pipeline check results."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import List, Literal

from pipewatch.checker import CheckResult, CheckStatus

ReportFormat = Literal["text", "json", "csv"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _status_icon(status: CheckStatus) -> str:
    return {CheckStatus.OK: "✓", CheckStatus.STALE: "!", CheckStatus.FAILED: "✗"}.get(
        status, "?"
    )


def build_text_report(results: List[CheckResult], generated_at: datetime | None = None) -> str:
    """Return a human-readable summary report."""
    ts = (generated_at or _utcnow()).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [f"PipeWatch Report — {ts}", "-" * 44]
    for r in results:
        icon = _status_icon(r.status)
        lines.append(f"  [{icon}] {r.pipeline_name:<24} {r.status.value}")
        if r.message:
            lines.append(f"       {r.message}")
    lines.append("-" * 44)
    ok = sum(1 for r in results if r.status == CheckStatus.OK)
    lines.append(f"  {ok}/{len(results)} pipelines healthy")
    return "\n".join(lines)


def build_json_report(results: List[CheckResult], generated_at: datetime | None = None) -> str:
    """Return a JSON-encoded report."""
    ts = (generated_at or _utcnow()).isoformat()
    payload = {
        "generated_at": ts,
        "summary": {
            "total": len(results),
            "ok": sum(1 for r in results if r.status == CheckStatus.OK),
            "stale": sum(1 for r in results if r.status == CheckStatus.STALE),
            "failed": sum(1 for r in results if r.status == CheckStatus.FAILED),
        },
        "pipelines": [
            {"name": r.pipeline_name, "status": r.status.value, "message": r.message}
            for r in results
        ],
    }
    return json.dumps(payload, indent=2)


def build_csv_report(results: List[CheckResult], generated_at: datetime | None = None) -> str:
    """Return a CSV-encoded report."""
    ts = (generated_at or _utcnow()).isoformat()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["generated_at", "pipeline_name", "status", "message"])
    for r in results:
        writer.writerow([ts, r.pipeline_name, r.status.value, r.message or ""])
    return buf.getvalue()


def build_report(results: List[CheckResult], fmt: ReportFormat = "text") -> str:
    """Dispatch to the correct report builder based on *fmt*."""
    builders = {"text": build_text_report, "json": build_json_report, "csv": build_csv_report}
    if fmt not in builders:
        raise ValueError(f"Unsupported report format: {fmt!r}")
    return builders[fmt](results)
