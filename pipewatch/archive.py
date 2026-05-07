"""Archive module: mark pipelines as archived and filter them from active checks."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ArchiveEntry:
    pipeline: str
    archived_at: datetime
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "archived_at": self.archived_at.isoformat(),
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(data: dict) -> "ArchiveEntry":
        return ArchiveEntry(
            pipeline=data["pipeline"],
            archived_at=datetime.fromisoformat(data["archived_at"]),
            reason=data.get("reason"),
        )

    def summary(self) -> str:
        reason_part = f" ({self.reason})" if self.reason else ""
        return f"{self.pipeline} archived at {self.archived_at.isoformat()}{reason_part}"


class ArchiveStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: Dict[str, ArchiveEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text())
        self._entries = {
            k: ArchiveEntry.from_dict(v) for k, v in raw.items()
        }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def is_archived(self, pipeline: str) -> bool:
        return pipeline in self._entries

    def archive(self, pipeline: str, reason: Optional[str] = None) -> ArchiveEntry:
        entry = ArchiveEntry(pipeline=pipeline, archived_at=_utcnow(), reason=reason)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def restore(self, pipeline: str) -> bool:
        if pipeline not in self._entries:
            return False
        del self._entries[pipeline]
        self._save()
        return True

    def all(self) -> List[ArchiveEntry]:
        return list(self._entries.values())

    def get(self, pipeline: str) -> Optional[ArchiveEntry]:
        return self._entries.get(pipeline)
