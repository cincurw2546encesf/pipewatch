"""Sliding-window success-rate checker for pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.history import HistoryEntry, HistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WindowResult:
    pipeline: str
    window_hours: int
    total: int
    ok: int
    failed: int
    success_rate: float  # 0.0 – 1.0
    threshold: float     # minimum acceptable success rate
    violated: bool

    def summary(self) -> str:
        pct = f"{self.success_rate * 100:.1f}%"
        status = "VIOLATED" if self.violated else "OK"
        return (
            f"{self.pipeline}: {pct} success over last {self.window_hours}h "
            f"({self.ok}/{self.total} runs) [{status}]"
        )


def check_window(
    pipeline: str,
    store: HistoryStore,
    window_hours: int = 24,
    threshold: float = 0.8,
    now_fn=None,
) -> Optional[WindowResult]:
    """Return a WindowResult for *pipeline* or None if no history exists."""
    now = (now_fn or _utcnow)()
    cutoff = now - timedelta(hours=window_hours)

    entries: List[HistoryEntry] = [
        e for e in store.get(pipeline)
        if e.checked_at >= cutoff
    ]

    if not entries:
        return None

    total = len(entries)
    ok = sum(1 for e in entries if e.status == "ok")
    failed = total - ok
    rate = ok / total if total else 0.0

    return WindowResult(
        pipeline=pipeline,
        window_hours=window_hours,
        total=total,
        ok=ok,
        failed=failed,
        success_rate=rate,
        threshold=threshold,
        violated=rate < threshold,
    )


def check_all_windows(
    pipelines: List[str],
    store: HistoryStore,
    window_hours: int = 24,
    threshold: float = 0.8,
    now_fn=None,
) -> List[WindowResult]:
    """Run window checks for every pipeline that has history."""
    results = []
    for name in pipelines:
        r = check_window(name, store, window_hours, threshold, now_fn)
        if r is not None:
            results.append(r)
    return results
