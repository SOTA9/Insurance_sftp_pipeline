from __future__ import annotations

import logging
import uuid
import datetime
from typing import List, Optional

from google.cloud import bigquery

from pipeline.config import settings

logger = logging.getLogger(__name__)

# BQ schema for pipeline_audit.ingestion_runs

SCHEMA = [
    bigquery.SchemaField("run_id",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("business_date", "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("gcs_uri",       "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("file_type",     "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("rows_valid",    "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("rows_dropped",  "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("ingested_at",   "TIMESTAMP", mode="REQUIRED"),
    # ADDED: identifies which SFTP directory this file came from
    bigquery.SchemaField("dir_id",        "STRING",    mode="NULLABLE"),
    # ADDED: full GCS prefix for traceability
    bigquery.SchemaField("gcs_prefix",    "STRING",    mode="NULLABLE"),
]

def write_audit(
    business_date: str,
    gcs_uris: List[str],
    stats: List[dict],
    dir_id: Optional[str] = None,
) -> None:

    client   = bigquery.Client(project=settings.GCS_PROJECT)
    table_id = f"{settings.GCS_PROJECT}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"
    run_id   = str(uuid.uuid4())
    now      = datetime.datetime.utcnow().isoformat()

    rows = []
    for uri, stat in zip(gcs_uris, stats):
        # Extract dir_id from GCS URI path if not explicitly provided
        # Pattern: gs://bucket/bronze/{dir_id}/{entity}/year=.../...
        inferred_dir_id = dir_id
        if inferred_dir_id is None:
            parts = uri.replace("gs://", "").split("/")
            # parts[0]=bucket, parts[1]=bronze, parts[2]=dir_id
            if len(parts) >= 3 and parts[1] == "bronze":
                inferred_dir_id = parts[2]

        # Extract gcs_prefix: everything up to but not including the filename
        gcs_prefix = "/".join(uri.replace("gs://", "").split("/")[:-1])

        rows.append({
            "run_id":        run_id,
            "business_date": business_date,
            "gcs_uri":       uri,
            "file_type":     stat.get("file_type", ""),
            "rows_valid":    stat.get("rows_valid", 0),
            "rows_dropped":  stat.get("rows_dropped", 0),
            "ingested_at":   now,
            "dir_id":        inferred_dir_id or "",
            "gcs_prefix":    gcs_prefix,
        })

    if not rows:
        logger.info("No audit rows to write (no files ingested this run).")
        return

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error("BigQuery audit insert errors: %s", errors)
    else:
        logger.info(
            "Audit written: run_id=%s rows=%d dirs=%s",
            run_id, len(rows),
            list({r["dir_id"] for r in rows if r["dir_id"]}),
        )