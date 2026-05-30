"""Failure callbacks that page the on-call when a task fails.

Wired up via `on_failure_callback` on each task (or on the DAG default_args)
in the fixed DAG. The callback receives Airflow's task context dict and
sends a Slack message via webhook (no API token needed - webhook URL is
the credential).

Slack webhook URL is read from the SLACK_ALERT_WEBHOOK_URL environment
variable. If it's unset the callback logs a warning and returns without
raising - we don't want a misconfigured alerting integration to crash the
task on top of whatever caused the original failure.
"""

import json
import os

from plugins.logging_config import get_logger

log = get_logger(__name__)


def _build_slack_blocks(context: dict) -> dict:
    """Translate Airflow task context into Slack Block Kit message."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = getattr(ti, "dag_id", "<unknown>")
    task_id = getattr(ti, "task_id", "<unknown>")
    execution_date = getattr(ti, "execution_date", None)
    try_number = getattr(ti, "try_number", 1)
    log_url = getattr(ti, "log_url", "")

    summary = f":rotating_light: *{dag_id} / {task_id}* failed"
    if try_number > 1:
        summary += f" (try {try_number})"

    fields = [
        {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
        {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
        {"type": "mrkdwn", "text": f"*Try:*\n{try_number}"},
        {"type": "mrkdwn", "text": f"*Execution:*\n{execution_date}"},
    ]
    if exception:
        fields.append({"type": "mrkdwn", "text": f"*Exception:*\n```{str(exception)[:300]}```"})

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
        {"type": "section", "fields": fields},
    ]
    if log_url:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Open Airflow logs"},
                "url": log_url,
            }],
        })
    return {"blocks": blocks}


def slack_on_failure(context: dict) -> None:
    """Post a Slack alert when a task fails.

    Called by Airflow with the task context dict. Logs the alert
    contents as a structured event regardless of webhook success so we
    have a record in CloudWatch even if Slack is down.
    """
    payload = _build_slack_blocks(context)

    # Always log the structured event - this is the audit trail
    log.error(
        "task_failed",
        extra={
            "dag_id": getattr(context.get("task_instance"), "dag_id", None),
            "task_id": getattr(context.get("task_instance"), "task_id", None),
            "try_number": getattr(context.get("task_instance"), "try_number", None),
            "exception": str(context.get("exception"))[:300] if context.get("exception") else None,
        },
    )

    webhook = os.environ.get("SLACK_ALERT_WEBHOOK_URL")
    if not webhook:
        log.warning("slack_webhook_not_configured", extra={"hint": "Set SLACK_ALERT_WEBHOOK_URL env var to enable Slack alerts"})
        return

    try:
        import requests
        resp = requests.post(webhook, json=payload, timeout=5)
        resp.raise_for_status()
        log.info("slack_alert_posted", extra={"status_code": resp.status_code})
    except Exception as e:
        # Never let alerting failures crash the task - it's already failing
        log.error("slack_alert_failed", extra={"error": str(e)[:200]})
