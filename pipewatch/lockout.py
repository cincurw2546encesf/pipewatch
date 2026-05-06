"""Lockout: temporarily block a pipeline from alerting after repeated failures."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LockoutEntry:
    pipeline: str
    locked_until: datetime
    reason: str = ""

    def is_locked(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        return now < self.locked_until

    def seconds_remaining(self, now: Optional[datetime] = None) -> float:
        now = now or _utcnow()
        delta = (self.locked_until - now).total_seconds()
        return max(0.0, delta)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "locked_until": self.locked_until.isoformat(),
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LockoutEntry":
        return cls(
            pipeline=data["pipeline"],
            locked_until=datetime.fromisoformat(data["locked_until"]),
            reason=data.get("reason", ""),
        )


class LockoutStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, LockoutEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text())
        self._entries = {
            k: LockoutEntry.from_dict(v) for k, v in raw.items()
        }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def lock(self, pipeline: str, duration_seconds: float, reason: str = "") -> LockoutEntry:
        until = _utcnow().__class__.fromtimestamp(
            _utcnow().timestamp() + duration_seconds, tz=timezone.utc
        )
        entry = LockoutEntry(pipeline=pipeline, locked_until=until, reason=reason)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def unlock(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def is_locked(self, pipeline: str) -> bool:
        entry = self._entries.get(pipeline)
        return entry is not None and entry.is_locked()

    def get(self, pipeline: str) -> Optional[LockoutEntry]:
        return self._entries.get(pipeline)

    def all_entries(self) -> list[LockoutEntry]:
        return list(self._entries.values())
