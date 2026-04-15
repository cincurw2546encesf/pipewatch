"""Alert dispatching for pipewatch pipeline health checks."""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import List, Optional

from pipewatch.checker import CheckResult, CheckStatus

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    smtp_host: str = "localhost"
    smtp_port: int = 25
    from_addr: str = "pipewatch@localhost"
    to_addrs: List[str] = None
    subject_prefix: str = "[pipewatch]"

    def __post_init__(self):
        if self.to_addrs is None:
            self.to_addrs = []


def _build_subject(result: CheckResult, prefix: str) -> str:
    status_tag = result.status.value.upper()
    return f"{prefix} {status_tag}: {result.pipeline_name}"


def _build_body(result: CheckResult) -> str:
    lines = [
        f"Pipeline : {result.pipeline_name}",
        f"Status   : {result.status.value}",
        f"Message  : {result.message}",
    ]
    if result.last_run is not None:
        lines.append(f"Last run : {result.last_run.isoformat()} UTC")
    else:
        lines.append("Last run : never")
    return "\n".join(lines)


def send_email_alert(
    result: CheckResult,
    cfg: AlertConfig,
    smtp_factory=None,
) -> bool:
    """Send an e-mail alert for *result*.  Returns True on success."""
    if not cfg.to_addrs:
        logger.warning("No alert recipients configured – skipping e-mail.")
        return False

    msg = EmailMessage()
    msg["From"] = cfg.from_addr
    msg["To"] = ", ".join(cfg.to_addrs)
    msg["Subject"] = _build_subject(result, cfg.subject_prefix)
    msg.set_content(_build_body(result))

    factory = smtp_factory or smtplib.SMTP
    try:
        with factory(cfg.smtp_host, cfg.smtp_port) as server:
            server.send_message(msg)
        logger.info("Alert sent for pipeline '%s'.", result.pipeline_name)
        return True
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to send alert: %s", exc)
        return False


def dispatch_alerts(
    results: List[CheckResult],
    cfg: AlertConfig,
    smtp_factory=None,
) -> List[CheckResult]:
    """Send alerts for every non-OK result.  Returns the alerted results."""
    alerted = []
    for result in results:
        if result.status != CheckStatus.OK:
            if send_email_alert(result, cfg, smtp_factory=smtp_factory):
                alerted.append(result)
    return alerted
