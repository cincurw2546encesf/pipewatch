"""Point-in-time snapshot of pipeline check results."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import CheckResult, CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SnapshotEntry:
    pipeline: str
    status: str
    last_run: Optional[str]
    message: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "last_run": self.last_run,
            "message": self.message,
        }

    @classmethod
    def from_result(cls, result: CheckResult) -> "SnapshotEntry":
        return cls(
            pipeline=result.pipeline,
            status=result.status.value,
            last_run=result.last_run.isoformat() if result.last_run else None,
            message=str(result),
        )


@dataclass
class Snapshot:
    taken_at: str
    entries: List[SnapshotEntry] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "taken_at": self.taken_at,
            "entries": [e.to_dict() for e in self.entries],
        }


def take_snapshot(results: List[CheckResult], path: str, now_fn=_utcnow) -> Snapshot:
    """Persist a snapshot of current results to *path* and return it."""
    snap = Snapshot(
        taken_at=now_fn().isoformat(),
        entries=[SnapshotEntry.from_result(r) for r in results],
    )
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w") as fh:
        json.dump(snap.to_dict(), fh, indent=2)
    return snap


def load_snapshot(path: str) -> Optional[Snapshot]:
    """Load the most recent snapshot from *path*, or None if absent."""
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    entries = [
        SnapshotEntry(**e) for e in data.get("entries", [])
    ]
    return Snapshot(taken_at=data["taken_at"], entries=entries)
