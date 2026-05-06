"""Tombstone: mark a pipeline as permanently retired/decommissioned."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class TombstoneEntry:
    pipeline: str
    reason: str
    retired_at: datetime
    retired_by: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "reason": self.reason,
            "retired_at": self.retired_at.isoformat(),
            "retired_by": self.retired_by,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TombstoneEntry":
        return cls(
            pipeline=data["pipeline"],
            reason=data["reason"],
            retired_at=datetime.fromisoformat(data["retired_at"]),
            retired_by=data.get("retired_by"),
        )

    def summary(self) -> str:
        by = f" by {self.retired_by}" if self.retired_by else ""
        return (
            f"{self.pipeline}: retired{by} on "
            f"{self.retired_at.strftime('%Y-%m-%d %H:%M UTC')} — {self.reason}"
        )


class TombstoneStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, TombstoneEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text())
        self._entries = {
            k: TombstoneEntry.from_dict(v) for k, v in raw.items()
        }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def is_retired(self, pipeline: str) -> bool:
        return pipeline in self._entries

    def retire(self, pipeline: str, reason: str, retired_by: Optional[str] = None) -> TombstoneEntry:
        entry = TombstoneEntry(
            pipeline=pipeline,
            reason=reason,
            retired_at=_utcnow(),
            retired_by=retired_by,
        )
        self._entries[pipeline] = entry
        self._save()
        return entry

    def restore(self, pipeline: str) -> bool:
        if pipeline not in self._entries:
            return False
        del self._entries[pipeline]
        self._save()
        return True

    def get(self, pipeline: str) -> Optional[TombstoneEntry]:
        return self._entries.get(pipeline)

    def all(self) -> list[TombstoneEntry]:
        return list(self._entries.values())
