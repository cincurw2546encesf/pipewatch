"""Notification routing: decides which alert channels to use and dispatches."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import AlertConfig, send_email_alert
from pipewatch.checker import CheckResult, CheckStatus

logger = logging.getLogger(__name__)


@dataclass
class NotificationResult:
    pipeline: str
    channel: str
    success: bool
    error: Optional[str] = None

    def __str__(self) -> str:
        status = "OK" if self.success else f"FAILED ({self.error})"
        return f"[{self.channel}] {self.pipeline}: {status}"


@dataclass
class NotificationSummary:
    sent: List[NotificationResult] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    @property
    def total_sent(self) -> int:
        return len(self.sent)

    @property
    def total_failed(self) -> int:
        return sum(1 for r in self.sent if not r.success)


def should_notify(result: CheckResult) -> bool:
    """Return True when a result warrants an alert notification."""
    return result.status in (CheckStatus.STALE, CheckStatus.FAILED)


def dispatch_notifications(
    results: List[CheckResult],
    alert_cfg: Optional[AlertConfig],
) -> NotificationSummary:
    """Route check results to configured alert channels."""
    summary = NotificationSummary()

    for result in results:
        if not should_notify(result):
            summary.skipped.append(result.pipeline)
            continue

        if alert_cfg is not None and alert_cfg.enabled:
            notif = _try_email(result, alert_cfg)
            summary.sent.append(notif)
        else:
            logger.debug("No alert channel configured for %s", result.pipeline)
            summary.skipped.append(result.pipeline)

    return summary


def _try_email(result: CheckResult, alert_cfg: AlertConfig) -> NotificationResult:
    try:
        send_email_alert(result, alert_cfg)
        logger.info("Email alert sent for pipeline '%s'", result.pipeline)
        return NotificationResult(pipeline=result.pipeline, channel="email", success=True)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send email for '%s': %s", result.pipeline, exc)
        return NotificationResult(
            pipeline=result.pipeline, channel="email", success=False, error=str(exc)
        )
