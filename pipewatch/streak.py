"""Track consecutive success/failure streaks per pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.checker import CheckResult, CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StreakEntry:
    pipeline: str
    current_status: str          # "ok" | "fail"
    count: int = 1
    started_at: str = field(default_factory=lambda: _utcnow().isoformat())

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "current_status": self.current_status,
            "count": self.count,
            "started_at": self.started_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StreakEntry":
        return cls(
            pipeline=d["pipeline"],
            current_status=d["current_status"],
            count=d["count"],
            started_at=d["started_at"],
        )


@dataclass
class StreakResult:
    pipeline: str
    current_status: str
    count: int
    started_at: str
    is_concerning: bool  # True when failing streak >= threshold

    def summary(self) -> str:
        icon = "✅" if self.current_status == "ok" else "❌"
        concern = " ⚠️" if self.is_concerning else ""
        return (
            f"{icon} {self.pipeline}: {self.current_status} streak "
            f"x{self.count} (since {self.started_at}){concern}"
        )


class StreakStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, StreakEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {
                k: StreakEntry.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2)
        )

    def get(self, pipeline: str) -> Optional[StreakEntry]:
        return self._data.get(pipeline)

    def record(self, pipeline: str, status: str) -> StreakEntry:
        existing = self._data.get(pipeline)
        if existing and existing.current_status == status:
            existing.count += 1
        else:
            self._data[pipeline] = StreakEntry(
                pipeline=pipeline, current_status=status
            )
        self._save()
        return self._data[pipeline]

    def all(self) -> list[StreakEntry]:
        return list(self._data.values())


def check_streak(
    result: CheckResult,
    store: StreakStore,
    fail_threshold: int = 3,
) -> StreakResult:
    status = "ok" if result.status == CheckStatus.OK else "fail"
    entry = store.record(result.pipeline, status)
    concerning = entry.current_status == "fail" and entry.count >= fail_threshold
    return StreakResult(
        pipeline=result.pipeline,
        current_status=entry.current_status,
        count=entry.count,
        started_at=entry.started_at,
        is_concerning=concerning,
    )


def check_all_streaks(
    results: list[CheckResult],
    store: StreakStore,
    fail_threshold: int = 3,
) -> list[StreakResult]:
    return [check_streak(r, store, fail_threshold) for r in results]
