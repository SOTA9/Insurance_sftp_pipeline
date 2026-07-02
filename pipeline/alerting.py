from __future__ import annotations

import logging
from typing import Optional

import requests

from pipeline.config import settings

logger = logging.getLogger(__name__)


def notify_slack(message: str) -> None:
    """Post a message to the configured Slack webhook. Unchanged."""
    webhook = settings.SLACK_WEBHOOK_URL
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL not configured — skipping notification.")
        return
    url  = webhook.get_secret_value()
    resp = requests.post(url, json={"text": message}, timeout=10)
    resp.raise_for_status()
    logger.info("Slack notification sent.")


def on_failure_callback(context: dict) -> None:
    """
    UPDATED: extracts dir_id from the task_id when the failing task
    belongs to a directory TaskGroup and includes it in the alert.

    Task ID patterns:
      ingest_inbound.check_inbound    - Dir: inbound
      ingest_reports.bronze_reports   - Dir: reports
      transform_to_silver             - Dir: (none — cross-dir task)
      load_to_gold                    - Dir: (none)
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    ds      = context["ds"]
    exc     = context.get("exception", "Unknown error")
    run_id  = context.get("run_id", "unknown")

    # Extract dir_id from task_id
    dir_id  = _extract_dir_id(task_id)
    dir_tag = f" | Dir: `{dir_id}`" if dir_id else ""

    msg = (
        f":x: *InsureFlow Pipeline FAILED*\n"
        f"DAG: `{dag_id}` | Task: `{task_id}`{dir_tag}\n"
        f"Date: `{ds}` | Run: `{run_id}`\n"
        f"Error: ```{str(exc)[:500]}```"
    )
    logger.error(
        "Task failure — dag=%s task=%s dir=%s date=%s",
        dag_id, task_id, dir_id, ds,
    )
    notify_slack(msg)


def notify_directory_summary(
    dir_id: str,
    label: str,
    business_date: str,
    new_files: int,
    uploaded: int,
    is_first_run: bool,
) -> None:
    """
    NEW: Post a per-directory completion summary to Slack.

    Called at the end of each directory's upload_bronze task so the
    team can see per-directory stats independently of overall pipeline.

    Example Slack message:
       [inbound] Operational files — 2026-05-14
      Mode: INCREMENTAL | New files: 4 | Uploaded: 4
    """
    mode = "FULL LOAD" if is_first_run else "INCREMENTAL"
    icon = ":white_check_mark:" if uploaded > 0 else ":information_source:"

    if uploaded == 0:
        msg = (
            f"{icon} *[{dir_id}] {label}* — `{business_date}`\n"
            f"Mode: {mode} | No new files — all up to date."
        )
    else:
        msg = (
            f"{icon} *[{dir_id}] {label}* — `{business_date}`\n"
            f"Mode: {mode} | New files: {new_files} | Uploaded to bronze: {uploaded}"
        )

    notify_slack(msg)


def _extract_dir_id(task_id: str) -> Optional[str]:
    """
    Extract dir_id from a task_id belonging to a directory TaskGroup.

    Airflow TaskGroup task IDs follow pattern:
      {group_id}.{task_id}  e.g.  ingest_inbound.check_inbound

    group_id pattern is:  ingest_{dir_id}
    """
    if "." in task_id:
        group = task_id.split(".")[0]
        if group.startswith("ingest_"):
            return group.replace("ingest_", "")
    return None