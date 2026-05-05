"""Pause/resume pipelines to temporarily exclude them from checks."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PauseEntry:
    pipeline: str
    paused_at: datetime
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "paused_at": self.paused_at.isoformat(),
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(d: dict) -> "PauseEntry":
        return PauseEntry(
            pipeline=d["pipeline"],
            paused_at=datetime.fromisoformat(d["paused_at"]),
            reason=d.get("reason"),
        )


class PauseStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: Dict[str, PauseEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text())
        self._entries = {
            name: PauseEntry.from_dict(entry)
            for name, entry in raw.items()
        }

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def is_paused(self, pipeline: str) -> bool:
        return pipeline in self._entries

    def pause(self, pipeline: str, reason: Optional[str] = None) -> PauseEntry:
        entry = PauseEntry(pipeline=pipeline, paused_at=_utcnow(), reason=reason)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def resume(self, pipeline: str) -> bool:
        if pipeline not in self._entries:
            return False
        del self._entries[pipeline]
        self._save()
        return True

    def all_paused(self) -> List[PauseEntry]:
        return list(self._entries.values())
