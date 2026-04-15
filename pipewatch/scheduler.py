"""Scheduler: periodically runs pipeline checks and fires alerts."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.alerts import AlertConfig, send_email_alert
from pipewatch.checker import CheckResult, CheckStatus, check_all
from pipewatch.config import AppConfig
from pipewatch.state import StateStore

logger = logging.getLogger(__name__)


@dataclass
class SchedulerStats:
    """Accumulated statistics across scheduler ticks."""

    ticks: int = 0
    alerts_sent: int = 0
    errors: int = 0
    last_results: List[CheckResult] = field(default_factory=list)


def _should_alert(result: CheckResult) -> bool:
    return result.status in (CheckStatus.STALE, CheckStatus.FAILED)


def run_once(
    app_cfg: AppConfig,
    store: StateStore,
    alert_cfg: Optional[AlertConfig] = None,
    *,
    _now_fn: Optional[Callable] = None,
) -> List[CheckResult]:
    """Run all pipeline checks once and send alerts for unhealthy pipelines."""
    results = check_all(app_cfg.pipelines, store, _now_fn=_now_fn)
    if alert_cfg:
        for result in results:
            if _should_alert(result):
                try:
                    send_email_alert(result, alert_cfg)
                    logger.info("Alert sent for pipeline '%s'.", result.pipeline_name)
                except Exception as exc:  # pragma: no cover
                    logger.error(
                        "Failed to send alert for '%s': %s", result.pipeline_name, exc
                    )
    return results


def run_scheduler(
    app_cfg: AppConfig,
    store: StateStore,
    interval_seconds: int = 60,
    alert_cfg: Optional[AlertConfig] = None,
    *,
    max_ticks: Optional[int] = None,
) -> SchedulerStats:  # pragma: no cover
    """Block and run checks on *interval_seconds* cadence.

    Pass *max_ticks* (int) to stop after a fixed number of iterations —
    useful for integration tests and one-shot daemon runs.
    """
    stats = SchedulerStats()
    logger.info(
        "Scheduler started — interval=%ds pipelines=%d",
        interval_seconds,
        len(app_cfg.pipelines),
    )
    while True:
        try:
            stats.last_results = run_once(app_cfg, store, alert_cfg)
            stats.ticks += 1
            unhealthy = sum(1 for r in stats.last_results if _should_alert(r))
            if unhealthy:
                stats.alerts_sent += unhealthy
            logger.info(
                "Tick %d complete — %d pipeline(s) checked, %d unhealthy.",
                stats.ticks,
                len(stats.last_results),
                unhealthy,
            )
        except Exception as exc:
            stats.errors += 1
            logger.error("Scheduler tick error: %s", exc)

        if max_ticks is not None and stats.ticks >= max_ticks:
            break
        time.sleep(interval_seconds)

    return stats
