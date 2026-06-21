"""
Incremental load state for the Silver -> Gold (BigQuery) layer.

Tracks which (table, date) combinations have already been loaded
into BigQuery fact tables. Dimension and aggregate tables are always
full-refresh so they do not need state tracking.

State file location per fact table:
  gs://{bucket}/_state/gold/{table_name}/loaded_dates.json

  e.g.
    gs://insureflow-datalake-prod/_state/gold/fact_premiums/loaded_dates.json
    gs://insureflow-datalake-prod/_state/gold/fact_claims/loaded_dates.json
    gs://insureflow-datalake-prod/_state/gold/fact_reinsurance/loaded_dates.json

State file format:
  {
    "table": "fact_claims",
    "last_updated": "2026-05-16T07:50:00Z",
    "loaded_dates": ["2026-05-14", "2026-05-15", "2026-05-16"]
  }

Idempotency for facts:
  Even if a date is already in state, force_reload=True triggers a
  DELETE partition + re-INSERT. This handles reruns after a silver fix.

Dimensions and aggregates: no state needed (WRITE_TRUNCATE is idempotent).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_STATE_ROOT = "_state"


class GoldState:
    """
    Manages incremental load state for one BigQuery fact table.

    Usage:
        state = GoldState(gcs_bucket="...", table_name="fact_claims", ...)

        if not state.is_loaded("2026-05-14"):
            # ... load the BQ partition ...
            state.mark_loaded("2026-05-14")
            state.save()
    """

    def __init__(
        self,
        gcs_bucket: str,
        table_name: str,
        gcs_project: str,
        sa_key_path: Optional[Path] = None,
    ):
        self.gcs_bucket  = gcs_bucket
        self.table_name  = table_name
        self._blob_name  = f"{_STATE_ROOT}/gold/{table_name}/loaded_dates.json"

        self._client = self._make_client(gcs_project, sa_key_path)
        self._bucket = self._client.bucket(gcs_bucket)
        self._data: dict = self._load()

    @staticmethod
    def _make_client(project: str, sa_key_path: Optional[Path]):
        from google.cloud import storage
        if sa_key_path:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(str(sa_key_path))
            return storage.Client(project=project, credentials=creds)
        return storage.Client(project=project)

    def _load(self) -> dict:
        blob = self._bucket.blob(self._blob_name)
        try:
            data = json.loads(blob.download_as_text())
            n    = len(data.get("loaded_dates", []))
            logger.info("[gold_state/%s] Loaded: %d dates already in BQ.", self.table_name, n)
            return data
        except Exception:
            logger.info("[gold_state/%s] No state — first load.", self.table_name)
            return {"table": self.table_name, "loaded_dates": []}

    # Query

    @property
    def loaded_dates(self) -> set[str]:
        return set(self._data.get("loaded_dates", []))

    @property
    def is_first_run(self) -> bool:
        return len(self.loaded_dates) == 0

    def is_loaded(self, date_str: str) -> bool:
        return date_str in self.loaded_dates

    def needs_load(self, date_str: str, force_reload: bool = False) -> bool:
        if force_reload:
            return True
        return not self.is_loaded(date_str)

    # Update

    def mark_loaded(self, date_str: str) -> None:
        dates = self.loaded_dates
        dates.add(date_str)
        self._data["loaded_dates"] = sorted(dates)

    def save(self) -> None:
        self._data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._data["table"]        = self.table_name

        blob = self._bucket.blob(self._blob_name)
        blob.upload_from_string(
            json.dumps(self._data, indent=2),
            content_type="application/json",
        )
        logger.info(
            "[gold_state/%s] Saved → gs://%s/%s (%d dates).",
            self.table_name, self.gcs_bucket, self._blob_name,
            len(self._data.get("loaded_dates", [])),
        )

    def reset(self) -> None:
        self._data = {"table": self.table_name, "loaded_dates": []}
        self.save()
        logger.warning(
            "[gold_state/%s] State RESET. All partitions will be reloaded.",
            self.table_name,
        )

    def summary(self) -> dict:
        return {
            "table":         self.table_name,
            "dates_loaded":  len(self.loaded_dates),
            "is_first_run":  self.is_first_run,
            "last_updated":  self._data.get("last_updated", "never"),
            "state_blob":    f"gs://{self.gcs_bucket}/{self._blob_name}",
        }