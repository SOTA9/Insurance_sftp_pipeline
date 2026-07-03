from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Date helpers (unchanged)

def get_business_date(context: dict) -> date:
    """Return the logical business date from the Airflow context (ds field)."""
    return date.fromisoformat(context["ds"])


def format_business_date(business_date: date) -> str:
    """Return compact YYYYMMDD string used in filenames e.g. 20260514."""
    return business_date.strftime("%Y%m%d")


# XCom helpers (unchanged)

def push_xcom(context: dict, key: str, value: Any) -> None:
    context["task_instance"].xcom_push(key=key, value=value)
    logger.debug("XCom pushed: key=%s", key)


def pull_xcom(context: dict, task_id: str, key: str = "return_value") -> Any:
    value = context["task_instance"].xcom_pull(task_ids=task_id, key=key)
    if value is None:
        raise ValueError(
            f"XCom pull returned None for task_id={task_id!r}, key={key!r}. "
            "Check that the upstream task ran successfully."
        )
    return value


# Callbacks

def on_failure_callback(context: dict) -> None:
    """
    UPDATED: extracts dir_id from the task_id when the failing task
    belongs to a directory TaskGroup.

    Task ID patterns in this DAG:
      ingest_inbound.check_inbound    : dir_id = inbound
      ingest_inbound.download_inbound : dir_id = inbound
      ingest_reports.bronze_reports   : dir_id = reports
      transform_to_silver             : dir_id = None (cross-dir task)
      load_to_gold                    : dir_id = None

    The dir_id is extracted by splitting on "." and checking known prefixes.
    This lets the on-call engineer immediately know which SFTP directory failed.
    """
    from pipeline.alerting import notify_slack

    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    ds      = context["ds"]
    exc     = context.get("exception", "Unknown error")
    run_id  = context.get("run_id", "unknown")

    # Extract dir_id from task_id if this is a directory-level task
    # e.g. "ingest_inbound.check_inbound" → dir_id = "inbound"
    dir_id = _extract_dir_id_from_task(task_id)
    dir_tag = f" | Dir: `{dir_id}`" if dir_id else ""

    msg = (
        f":x: *InsureFlow Pipeline FAILED*\n"
        f"DAG: `{dag_id}` | Task: `{task_id}`{dir_tag}\n"
        f"Date: `{ds}` | Run: `{run_id}`\n"
        f"Error: ```{str(exc)[:500]}```"
    )
    logger.error(
        "Task failure — dag=%s task=%s dir=%s date=%s exc=%s",
        dag_id, task_id, dir_id, ds, exc,
    )
    notify_slack(msg)


def _extract_dir_id_from_task(task_id: str) -> Optional[str]:
    """
    Extracts the directory ID from a task_id belonging to a TaskGroup.

    Examples:
      "ingest_inbound.check_inbound"    : "inbound"
      "ingest_reports.download_reports" : "reports"
      "transform_to_silver"             : None
      "load_to_gold"                    : None
    """
    # TaskGroup tasks follow pattern: ingest_{dir_id}.{task_name}_{dir_id}
    if "." in task_id:
        group_part = task_id.split(".")[0]  # e.g. "ingest_inbound"
        if group_part.startswith("ingest_"):
            return group_part.replace("ingest_", "")
    return None


def on_sla_miss_callback(
    dag,
    task_list: str,
    blocking_task_list: str,
    slas,
    blocking_tis,
) -> None:
    """Unchanged — fires when any task misses its SLA window."""
    from pipeline.alerting import notify_slack

    msg = (
        f":warning: *InsureFlow SLA MISSED*\n"
        f"DAG: `{dag.dag_id}`\n"
        f"Missed: `{task_list}`\n"
        f"Blocking: `{blocking_task_list}`"
    )
    logger.warning("SLA miss — dag=%s tasks=%s", dag.dag_id, task_list)
    notify_slack(msg)


# Logging helpers

def log_pipeline_banner(
    dag_id: str,
    business_date: date,
    dir_id: Optional[str] = None,
) -> None:
    """
    UPDATED: accepts optional dir_id so each directory TaskGroup can
    emit its own banner showing which directory it is processing.

    Without dir_id (called from the top-level DAG):
      InsureFlow DataPipeline | DAG: insurance_sftp_gcs_ingestion | Date: 2026-05-14

    With dir_id (called from inside ingest_inbound TaskGroup):
      InsureFlow DataPipeline | DAG: ... | Dir: inbound | Date: 2026-05-14
    """
    dir_line = f"\n  Dir    : {dir_id}" if dir_id else ""
    logger.info(
        "\n%s\n  InsureFlow DataPipeline\n  DAG    : %s%s\n  Date   : %s\n  UTC    : %s\n%s",
        "=" * 60,
        dag_id,
        dir_line,
        business_date.isoformat(),
        datetime.utcnow().isoformat(timespec="seconds"),
        "=" * 60,
    )


def log_directory_summary(sftp_dir) -> None:
    """
    NEW: logs the full config of one SFTPDirectory at the start of
    its TaskGroup. Extremely helpful during debugging and first-run
    validation — confirms the YAML was loaded correctly.

    Called at the top of each check_and_diff task.

    Example output:
      [inbound] Config loaded:
        remote_dir    : /inbound/
        use_pgp       : True
        use_checksum  : True
        gcs_prefix    : bronze/inbound
        state_key     : sftp/inbound
        entities      : ['policies', 'claims']
    """
    entity_list = sftp_dir.entities
    logger.info(
        "[%s] Config loaded:\n"
        "  remote_dir    : %s\n"
        "  use_pgp       : %s\n"
        "  use_checksum  : %s\n"
        "  gcs_prefix    : %s\n"
        "  state_key     : %s\n"
        "  entities      : %s",
        sftp_dir.dir_id,
        sftp_dir.remote_dir,
        sftp_dir.use_pgp,
        sftp_dir.use_checksum,
        sftp_dir.gcs_prefix,
        sftp_dir.state_key,
        entity_list,
    )


# Secret Manager helper

def get_secret(secret_id: str, project_id: str, version: str = "latest") -> str:
    """
    Fetch a secret from GCP Secret Manager.

    UPDATED DOCSTRING : multi-directory secret naming convention:

    Global secrets (shared, same for all directories):
      sftp-ssh-private-key   : RSA key for SFTP authentication (same server)
      sftp-host              : SFTP hostname
      sftp-user              : SFTP username
      airflow-fernet-key     : Airflow Fernet key
      slack-webhook-url      : Slack webhook

    Directory-scoped PGP secrets (one set per encrypted directory):
      inbound-pgp-private-key    : PGP key for /inbound/ files
      inbound-pgp-passphrase     : passphrase for /inbound/ PGP key
      (no PGP secrets for /reports/ because use_pgp=False)

    Falls back to the env var (SECRET_ID uppercased, hyphens->underscores)
    when Secret Manager is unavailable — allows local dev without GCP.
    """
    env_fallback = secret_id.upper().replace("-", "_")

    try:
        from google.cloud import secretmanager

        client   = secretmanager.SecretManagerServiceClient()
        name     = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        value    = response.payload.data.decode("utf-8").strip()
        logger.debug("Loaded secret '%s' from Secret Manager.", secret_id)
        return value

    except Exception as exc:
        fallback = os.environ.get(env_fallback)
        if fallback:
            logger.warning(
                "Secret Manager unavailable (%s). Using env var %s.", exc, env_fallback
            )
            return fallback
        raise RuntimeError(
            f"Secret '{secret_id}' unavailable from Secret Manager "
            f"and env var '{env_fallback}' is not set."
        ) from exc