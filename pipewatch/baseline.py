"""Baseline tracking: record and compare expected run durations."""
from __future__ import annotations
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


@dataclass
class BaselineEntry:
    pipeline: str
    avg_duration_seconds: float
    sample_count: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "BaselineEntry":
        return cls(**d)


class BaselineStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, BaselineEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: BaselineEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def get(self, pipeline: str) -> Optional[BaselineEntry]:
        return self._data.get(pipeline)

    def update(self, pipeline: str, duration_seconds: float) -> BaselineEntry:
        existing = self._data.get(pipeline)
        if existing is None:
            entry = BaselineEntry(pipeline=pipeline, avg_duration_seconds=duration_seconds, sample_count=1)
        else:
            n = existing.sample_count
            new_avg = (existing.avg_duration_seconds * n + duration_seconds) / (n + 1)
            entry = BaselineEntry(pipeline=pipeline, avg_duration_seconds=round(new_avg, 3), sample_count=n + 1)
        self._data[pipeline] = entry
        self._save()
        return entry

    def all(self) -> list[BaselineEntry]:
        return list(self._data.values())

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._data:
            del self._data[pipeline]
            self._save()
            return True
        return False
