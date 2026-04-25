"""Cooldown tracking — prevents re-alerting for a pipeline until a minimum
quiet period has elapsed after the last alert was sent."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CooldownEntry:
    pipeline: str
    alerted_at: datetime
    cooldown_seconds: int

    def is_cooling(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        elapsed = (now - self.alerted_at).total_seconds()
        return elapsed < self.cooldown_seconds

    def seconds_remaining(self, now: Optional[datetime] = None) -> float:
        now = now or _utcnow()
        elapsed = (now - self.alerted_at).total_seconds()
        return max(0.0, self.cooldown_seconds - elapsed)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "alerted_at": self.alerted_at.isoformat(),
            "cooldown_seconds": self.cooldown_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CooldownEntry":
        return cls(
            pipeline=data["pipeline"],
            alerted_at=datetime.fromisoformat(data["alerted_at"]),
            cooldown_seconds=int(data["cooldown_seconds"]),
        )


class CooldownStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: Dict[str, CooldownEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text())
        self._entries = {
            k: CooldownEntry.from_dict(v) for k, v in raw.items()
        }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def record_alert(self, pipeline: str, cooldown_seconds: int) -> CooldownEntry:
        entry = CooldownEntry(
            pipeline=pipeline,
            alerted_at=_utcnow(),
            cooldown_seconds=cooldown_seconds,
        )
        self._entries[pipeline] = entry
        self._save()
        return entry

    def is_cooling(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline)
        if entry is None:
            return False
        return entry.is_cooling(now=now)

    def get(self, pipeline: str) -> Optional[CooldownEntry]:
        return self._entries.get(pipeline)

    def reset(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def all_entries(self) -> list[CooldownEntry]:
        return list(self._entries.values())
