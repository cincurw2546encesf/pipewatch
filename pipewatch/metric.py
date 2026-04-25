"""Pipeline run duration metrics: track and summarise execution times."""
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MetricEntry:
    pipeline: str
    duration_seconds: float
    recorded_at: str  # ISO-8601

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "duration_seconds": self.duration_seconds,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "MetricEntry":
        return cls(
            pipeline=d["pipeline"],
            duration_seconds=float(d["duration_seconds"]),
            recorded_at=d["recorded_at"],
        )


@dataclass
class MetricSummary:
    pipeline: str
    count: int
    mean_seconds: float
    min_seconds: float
    max_seconds: float
    p95_seconds: float

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: count={self.count} "
            f"mean={self.mean_seconds:.1f}s min={self.min_seconds:.1f}s "
            f"max={self.max_seconds:.1f}s p95={self.p95_seconds:.1f}s"
        )


class MetricStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: List[MetricEntry] = []
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._entries = [MetricEntry.from_dict(r) for r in raw]

    def _save(self) -> None:
        self._path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def record(self, pipeline: str, duration_seconds: float) -> MetricEntry:
        entry = MetricEntry(
            pipeline=pipeline,
            duration_seconds=duration_seconds,
            recorded_at=_utcnow().isoformat(),
        )
        self._entries.append(entry)
        self._save()
        return entry

    def get(self, pipeline: str) -> List[MetricEntry]:
        return [e for e in self._entries if e.pipeline == pipeline]

    def summarise(self, pipeline: str) -> Optional[MetricSummary]:
        entries = self.get(pipeline)
        if not entries:
            return None
        durations = [e.duration_seconds for e in entries]
        sorted_d = sorted(durations)
        idx = max(0, int(len(sorted_d) * 0.95) - 1)
        return MetricSummary(
            pipeline=pipeline,
            count=len(durations),
            mean_seconds=statistics.mean(durations),
            min_seconds=min(durations),
            max_seconds=max(durations),
            p95_seconds=sorted_d[idx],
        )

    def clear(self, pipeline: str) -> int:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self._save()
        return before - len(self._entries)
