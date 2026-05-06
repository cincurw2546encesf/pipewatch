"""Decay scoring: penalise pipelines that repeatedly fail over time."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DecayResult:
    pipeline: str
    score: float          # 0.0 (healthy) – 1.0 (fully decayed)
    failure_count: int
    last_failure: Optional[datetime]
    exceeded: bool

    def summary(self) -> str:
        icon = "🔴" if self.exceeded else "🟡" if self.score > 0.3 else "🟢"
        ts = self.last_failure.isoformat() if self.last_failure else "never"
        return (
            f"{icon} {self.pipeline}: decay={self.score:.2f} "
            f"failures={self.failure_count} last_failure={ts}"
        )


@dataclass
class DecayEntry:
    failure_count: int = 0
    last_failure: Optional[str] = None   # ISO-8601

    def to_dict(self) -> dict:
        return {"failure_count": self.failure_count, "last_failure": self.last_failure}

    @classmethod
    def from_dict(cls, d: dict) -> "DecayEntry":
        return cls(failure_count=d.get("failure_count", 0),
                   last_failure=d.get("last_failure"))


class DecayStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, DecayEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: DecayEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def get(self, pipeline: str) -> DecayEntry:
        return self._data.get(pipeline, DecayEntry())

    def record_failure(self, pipeline: str, now_fn=_utcnow) -> None:
        entry = self._data.get(pipeline, DecayEntry())
        entry.failure_count += 1
        entry.last_failure = now_fn().isoformat()
        self._data[pipeline] = entry
        self._save()

    def reset(self, pipeline: str) -> None:
        self._data.pop(pipeline, None)
        self._save()

    def all_entries(self) -> dict[str, DecayEntry]:
        return dict(self._data)


def _compute_score(entry: DecayEntry, half_life_days: float = 7.0, now_fn=_utcnow) -> float:
    """Exponential decay score in [0, 1]. Decays toward 0 over time."""
    if entry.failure_count == 0 or not entry.last_failure:
        return 0.0
    last = datetime.fromisoformat(entry.last_failure)
    age_days = (now_fn() - last).total_seconds() / 86400.0
    raw = 1.0 - math.exp(-entry.failure_count / 3.0)
    decay = math.exp(-age_days / half_life_days)
    return round(raw * decay, 4)


def check_decay(
    pipeline,
    store: DecayStore,
    threshold: float = 0.6,
    half_life_days: float = 7.0,
    now_fn=_utcnow,
) -> Optional[DecayResult]:
    name = pipeline.name if hasattr(pipeline, "name") else str(pipeline)
    cfg = getattr(pipeline, "decay", None)
    if cfg is None:
        return None
    t = cfg.get("threshold", threshold) if isinstance(cfg, dict) else threshold
    hl = cfg.get("half_life_days", half_life_days) if isinstance(cfg, dict) else half_life_days
    entry = store.get(name)
    score = _compute_score(entry, half_life_days=hl, now_fn=now_fn)
    last = datetime.fromisoformat(entry.last_failure) if entry.last_failure else None
    return DecayResult(
        pipeline=name,
        score=score,
        failure_count=entry.failure_count,
        last_failure=last,
        exceeded=score >= t,
    )


def check_all_decay(pipelines, store: DecayStore, **kwargs) -> list[DecayResult]:
    results = []
    for p in pipelines:
        r = check_decay(p, store, **kwargs)
        if r is not None:
            results.append(r)
    return results
