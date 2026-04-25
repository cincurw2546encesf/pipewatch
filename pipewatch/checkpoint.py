"""Checkpoint tracking — record and query named progress markers for pipelines."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CheckpointEntry:
    pipeline: str
    name: str
    recorded_at: datetime
    metadata: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "name": self.name,
            "recorded_at": self.recorded_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CheckpointEntry":
        return cls(
            pipeline=d["pipeline"],
            name=d["name"],
            recorded_at=datetime.fromisoformat(d["recorded_at"]),
            metadata=d.get("metadata", {}),
        )

    def summary(self) -> str:
        ts = self.recorded_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        return f"{self.pipeline} | {self.name} @ {ts}"


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: List[CheckpointEntry] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._entries = []
            return
        raw = json.loads(self._path.read_text())
        self._entries = [CheckpointEntry.from_dict(r) for r in raw]

    def _save(self) -> None:
        self._path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def record(self, pipeline: str, name: str, metadata: Optional[Dict[str, str]] = None) -> CheckpointEntry:
        entry = CheckpointEntry(
            pipeline=pipeline,
            name=name,
            recorded_at=_utcnow(),
            metadata=metadata or {},
        )
        self._entries.append(entry)
        self._save()
        return entry

    def get(self, pipeline: str) -> List[CheckpointEntry]:
        return [e for e in self._entries if e.pipeline == pipeline]

    def latest(self, pipeline: str) -> Optional[CheckpointEntry]:
        matches = self.get(pipeline)
        return matches[-1] if matches else None

    def clear(self, pipeline: str) -> int:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self._save()
        return before - len(self._entries)

    def all_entries(self) -> List[CheckpointEntry]:
        return list(self._entries)
