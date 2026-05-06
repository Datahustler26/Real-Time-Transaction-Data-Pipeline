"""
Slack Alert Helpers
===================
Alerting module for pipeline failures, SLA breaches, and success notifications.
Sends rich Slack messages with task context and recovery suggestions.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import requests

from common.config import PipelineConfig

logger = logging.getLogger(__name__)
CONFIG = PipelineConfig()


def send_slack_message(
    message: str,
    blocks: Optional[list] = None,
    webhook_url: Optional[str] = None,
) -> bool:
    """
    Send a message to Slack via webhook.

    Args:
        message: Fallback text message.
        blocks: Optional Slack Block Kit blocks for rich formatting.
        webhook_url: Override webhook URL (defaults to config).

    Returns:
        True if message sent successfully, False otherwise.
    """
    url = webhook_url or CONFIG.SLACK_WEBHOOK_URL
    if not url:
        logger.warning("Slack webhook URL not configured — skipping alert")
        return False

    payload = {"text": message}
    if blocks:
        payload["blocks"] = blocks

    try:
        response = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code == 200:
            logger.info("Slack alert sent successfully")
            return True
        else:
            logger.error(f"Slack alert failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Slack alert exception: {e}")
        return False


def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow on_failure_callback — sends detailed failure alert to Slack.
    Includes task ID, execution date, exception, log URL, and retry info.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception details")
    try_number = task_instance.try_number if task_instance else 0
    max_tries = task_instance.max_tries if task_instance else 0
    log_url = task_instance.log_url if task_instance else ""

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🚨 Pipeline Task Failed", "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                {"type": "mrkdwn", "text": f"*Execution:*\n`{execution_date}`"},
                {"type": "mrkdwn", "text": f"*Attempt:*\n`{try_number}/{max_tries}`"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Error:*\n```{str(exception)[:500]}```"},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "📋 View Logs"},
                    "url": log_url or "http://localhost:8080",
                },
            ],
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏰ Alert generated at {datetime.utcnow().isoformat()}Z"},
            ],
        },
    ]

    send_slack_message(
        message=f"🚨 FAILED: {dag_id}.{task_id} at {execution_date}",
        blocks=blocks,
    )


def on_sla_miss_callback(
    dag, task_list, blocking_task_list, slas, blocking_tis
) -> None:
    """
    Airflow SLA miss callback — alerts when pipeline exceeds time threshold.
    """
    task_names = [t.task_id for t in task_list] if task_list else []

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "⚠️ Pipeline SLA Breach", "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{dag.dag_id}`"},
                {"type": "mrkdwn", "text": f"*SLA Threshold:*\n`{CONFIG.SLA_SECONDS}s`"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Affected Tasks:*\n{', '.join(task_names[:10])}",
            },
        },
    ]

    send_slack_message(
        message=f"⚠️ SLA BREACH: {dag.dag_id} exceeded {CONFIG.SLA_SECONDS}s",
        blocks=blocks,
    )


def send_success_alert(
    context: Dict[str, Any],
    metrics: Optional[Dict] = None,
) -> None:
    """Send pipeline success notification with summary metrics."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    execution_date = context.get("execution_date", "unknown")

    metrics_text = ""
    if metrics:
        metrics_text = "\n".join(
            f"• *{k.replace('_', ' ').title()}:* {v}" for k, v in metrics.items()
        )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "✅ Pipeline Completed Successfully", "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Execution:*\n`{execution_date}`"},
            ],
        },
    ]

    if metrics_text:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Metrics:*\n{metrics_text}"},
        })

    send_slack_message(
        message=f"✅ SUCCESS: {dag_id} completed at {execution_date}",
        blocks=blocks,
    )
