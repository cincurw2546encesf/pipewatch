"""Watermark tracking — record and check the high-water mark (latest successful run timestamp) for each pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WatermarkEntry:
    pipeline: str
    high_water: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "high_water": self.high_water.isoformat(),
        }

    @staticmethod
    def from_dict(d: dict) -> "WatermarkEntry":
        return WatermarkEntry(
            pipeline=d["pipeline"],
            high_water=datetime.fromisoformat(d["high_water"]),
        )


@dataclass
class WatermarkResult:
    pipeline: str
    high_water: Optional[datetime]
    regressed: bool
    previous: Optional[datetime]

    def summary(self) -> str:
        if self.high_water is None:
            return f"{self.pipeline}: no watermark recorded"
        if self.regressed:
            return (
                f"{self.pipeline}: REGRESSION — watermark moved backward "
                f"(was {self.previous.isoformat()}, now {self.high_water.isoformat()})"
            )
        return f"{self.pipeline}: watermark OK ({self.high_water.isoformat()})"


class WatermarkStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, WatermarkEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: WatermarkEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def get(self, pipeline: str) -> Optional[WatermarkEntry]:
        return self._data.get(pipeline)

    def update(self, pipeline: str, ts: datetime) -> WatermarkResult:
        existing = self._data.get(pipeline)
        previous = existing.high_water if existing else None
        regressed = previous is not None and ts < previous
        if not regressed:
            self._data[pipeline] = WatermarkEntry(pipeline=pipeline, high_water=ts)
            self._save()
        return WatermarkResult(
            pipeline=pipeline,
            high_water=ts if not regressed else previous,
            regressed=regressed,
            previous=previous,
        )

    def all(self) -> list[WatermarkEntry]:
        return list(self._data.values())

    def reset(self, pipeline: str) -> bool:
        if pipeline in self._data:
            del self._data[pipeline]
            self._save()
            return True
        return False
