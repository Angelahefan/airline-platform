import json
import logging
import os
import urllib.request
from datetime import datetime


logger = logging.getLogger(__name__)

SLACK_TIMEOUT_SECONDS = 10


def _format_failure_message(context: dict) -> str:
    ti = context.get("task_instance") or context.get("ti")

    dag_id = getattr(ti, "dag_id", "unknown_dag")
    task_id = getattr(ti, "task_id", "unknown_task")
    try_number = getattr(ti, "try_number", "?")
    log_url = getattr(ti, "log_url", None)
    run_id = context.get("run_id", "unknown_run")
    exception = context.get("exception")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        f"[ALERT] {dag_id}.{task_id} failed at {timestamp}",
        f"run_id={run_id} try={try_number}",
    ]

    if exception is not None:
        lines.append(f"exception: {exception!r}")

    if log_url:
        lines.append(f"log: {log_url}")

    return "\n".join(lines)


def _post_to_slack(webhook_url: str, message: str) -> None:
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    urllib.request.urlopen(
        request,
        timeout=SLACK_TIMEOUT_SECONDS,
    )


def notify_failure(context: dict) -> None:
    try:
        message = _format_failure_message(context)
    except Exception:
        logger.exception("Failed to format failure message")
        return

    logger.error("%s", message)

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — failure logged, no Slack alert sent")
        return

    try:
        _post_to_slack(webhook_url, message)
    except Exception:  # noqa: BLE001 — alerting must never re-fail the task
        logger.exception("Failed to send Slack failure alert")



