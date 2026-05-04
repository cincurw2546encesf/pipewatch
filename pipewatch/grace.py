"""Grace period tracking: suppress alerts during a pipeline's initial warm-up window."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class GraceEntry:
    pipeline: str
    registered_at: datetime
    grace_seconds: int

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        elapsed = (now - self.registered_at).total_seconds()
        return elapsed < self.grace_seconds

    def seconds_remaining(self, now: Optional[datetime] = None) -> float:
        now = now or _utcnow()
        elapsed = (now - self.registered_at).total_seconds()
        return max(0.0, self.grace_seconds - elapsed)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "registered_at": self.registered_at.isoformat(),
            "grace_seconds": self.grace_seconds,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "GraceEntry":
        return cls(
            pipeline=d["pipeline"],
            registered_at=datetime.fromisoformat(d["registered_at"]),
            grace_seconds=int(d["grace_seconds"]),
        )


class GraceStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, GraceEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text())
        self._entries = {k: GraceEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2))

    def register(self, pipeline: str, grace_seconds: int, now: Optional[datetime] = None) -> GraceEntry:
        entry = GraceEntry(
            pipeline=pipeline,
            registered_at=now or _utcnow(),
            grace_seconds=grace_seconds,
        )
        self._entries[pipeline] = entry
        self._save()
        return entry

    def is_in_grace(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline)
        if entry is None:
            return False
        return entry.is_active(now)

    def get(self, pipeline: str) -> Optional[GraceEntry]:
        return self._entries.get(pipeline)

    def remove(self, pipeline: str) -> bool:
        if pipeline not in self._entries:
            return False
        del self._entries[pipeline]
        self._save()
        return True

    def all_entries(self) -> list[GraceEntry]:
        return list(self._entries.values())
