"""Heartbeat tracking — records periodic pings from pipelines and detects missed beats."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class HeartbeatEntry:
    pipeline: str
    last_beat: datetime
    interval_seconds: int  # expected max gap between beats

    def is_alive(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        return (now - self.last_beat) <= timedelta(seconds=self.interval_seconds)

    def seconds_since(self, now: Optional[datetime] = None) -> float:
        now = now or _utcnow()
        return (now - self.last_beat).total_seconds()

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_beat": self.last_beat.isoformat(),
            "interval_seconds": self.interval_seconds,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "HeartbeatEntry":
        return cls(
            pipeline=d["pipeline"],
            last_beat=datetime.fromisoformat(d["last_beat"]),
            interval_seconds=int(d["interval_seconds"]),
        )


@dataclass
class HeartbeatReport:
    pipeline: str
    alive: bool
    seconds_since: float
    interval_seconds: int

    def __str__(self) -> str:
        status = "ALIVE" if self.alive else "MISSED"
        return (
            f"[{status}] {self.pipeline}: last beat {self.seconds_since:.0f}s ago "
            f"(interval {self.interval_seconds}s)"
        )


class HeartbeatStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, HeartbeatEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: HeartbeatEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def beat(self, pipeline: str, interval_seconds: int, now: Optional[datetime] = None) -> HeartbeatEntry:
        entry = HeartbeatEntry(
            pipeline=pipeline,
            last_beat=now or _utcnow(),
            interval_seconds=interval_seconds,
        )
        self._data[pipeline] = entry
        self._save()
        return entry

    def get(self, pipeline: str) -> Optional[HeartbeatEntry]:
        return self._data.get(pipeline)

    def check(self, pipeline: str, now: Optional[datetime] = None) -> Optional[HeartbeatReport]:
        entry = self._data.get(pipeline)
        if entry is None:
            return None
        now = now or _utcnow()
        return HeartbeatReport(
            pipeline=pipeline,
            alive=entry.is_alive(now),
            seconds_since=entry.seconds_since(now),
            interval_seconds=entry.interval_seconds,
        )

    def check_all(self, now: Optional[datetime] = None) -> list[HeartbeatReport]:
        now = now or _utcnow()
        return [self.check(p, now) for p in self._data]

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._data:
            del self._data[pipeline]
            self._save()
            return True
        return False

    def all_entries(self) -> list[HeartbeatEntry]:
        return list(self._data.values())
