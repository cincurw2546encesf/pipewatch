"""Suppression: temporarily suppress alerts for specific pipelines based on a time window or pattern."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SuppressionEntry:
    pipeline: str
    reason: str
    suppressed_until: datetime
    created_at: datetime = field(default_factory=_utcnow)

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        return now < self.suppressed_until

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "reason": self.reason,
            "suppressed_until": self.suppressed_until.isoformat(),
            "created_at": self.created_at.isoformat(),
        }

    @staticmethod
    def from_dict(d: dict) -> "SuppressionEntry":
        return SuppressionEntry(
            pipeline=d["pipeline"],
            reason=d["reason"],
            suppressed_until=datetime.fromisoformat(d["suppressed_until"]),
            created_at=datetime.fromisoformat(d["created_at"]),
        )


class SuppressionStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, SuppressionEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        data = json.loads(self._path.read_text())
        self._entries = {
            k: SuppressionEntry.from_dict(v) for k, v in data.items()
        }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def suppress(self, pipeline: str, reason: str, until: datetime) -> SuppressionEntry:
        entry = SuppressionEntry(pipeline=pipeline, reason=reason, suppressed_until=until)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def is_suppressed(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline)
        return entry is not None and entry.is_active(now)

    def get(self, pipeline: str) -> Optional[SuppressionEntry]:
        return self._entries.get(pipeline)

    def all_active(self, now: Optional[datetime] = None) -> list[SuppressionEntry]:
        return [e for e in self._entries.values() if e.is_active(now)]

    def purge_expired(self, now: Optional[datetime] = None) -> int:
        expired = [k for k, v in self._entries.items() if not v.is_active(now)]
        for k in expired:
            del self._entries[k]
        if expired:
            self._save()
        return len(expired)
