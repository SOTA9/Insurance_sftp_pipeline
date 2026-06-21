# pipeline/state/silver_state.py
"""
Incremental transformation state for the Bronze → Silver layer.

Tracks which (entity, date) combinations have already been transformed
and written as Parquet to the silver GCS prefix.

State file location per entity:
  gs://{bucket}/_state/silver/{entity}/processed_dates.json

  e.g.
    gs://insureflow-datalake-prod/_state/silver/policies/processed_dates.json
    gs://insureflow-datalake-prod/_state/silver/claims/processed_dates.json
    gs://insureflow-datalake-prod/_state/silver/premiums/processed_dates.json
    gs://insureflow-datalake-prod/_state/silver/reinsurance/processed_dates.json

State file format:
  {
    "entity": "policies",
    "last_updated": "2026-05-16T07:45:00Z",
    "processed_dates": ["2026-05-14", "2026-05-15", "2026-05-16"]
  }

First run: state file missing → processed_dates is empty →
  transform ALL available bronze dates for this entity.

Subsequent runs: only dates NOT in processed_dates are transformed.

force_reprocess=True: re-transform a date even if already in state.
  Use when you fix a transformer bug and need to rewrite a silver file.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_STATE_ROOT = "_state"


class SilverState:
    """
    Manages incremental transformation state for one silver entity.

    Usage:
        state = SilverState(gcs_bucket="...", entity="policies", ...)

        # Check if a date needs processing
        if not state.is_processed("2026-05-14"):
            # ... run the transformer ...
            state.mark_processed("2026-05-14")
            state.save()
    """

    def __init__(
        self,
        gcs_bucket: str,
        entity: str,
        gcs_project: str,
        sa_key_path: Optional[Path] = None,
    ):
        self.gcs_bucket = gcs_bucket
        self.entity     = entity
        self._blob_name = f"{_STATE_ROOT}/silver/{entity}/processed_dates.json"

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
            raw  = blob.download_as_text()
            data = json.loads(raw)
            n    = len(data.get("processed_dates", []))
            logger.info(
                "[silver_state/%s] Loaded: %d dates already transformed.",
                self.entity, n,
            )
            return data
        except Exception:
            logger.info(
                "[silver_state/%s] No state — first run, all dates will be processed.",
                self.entity,
            )
            return {"entity": self.entity, "processed_dates": []}

    # Query

    @property
    def processed_dates(self) -> set[str]:
        return set(self._data.get("processed_dates", []))

    @property
    def is_first_run(self) -> bool:
        return len(self.processed_dates) == 0

    def is_processed(self, date_str: str) -> bool:
        """date_str in ISO format: '2026-05-14'"""
        return date_str in self.processed_dates

    def get_unprocessed_dates(self, candidate_dates: list[str]) -> list[str]:
        """
        Return dates from candidate_dates not yet in state.
        candidate_dates: list of ISO date strings from bronze GCS scan.
        """
        processed = self.processed_dates
        new_dates  = [d for d in candidate_dates if d not in processed]

        if self.is_first_run:
            logger.info(
                "[silver_state/%s] FULL TRANSFORM: %d dates to process.",
                self.entity, len(new_dates),
            )
        else:
            logger.info(
                "[silver_state/%s] INCREMENTAL: %d new dates (of %d total bronze dates).",
                self.entity, len(new_dates), len(candidate_dates),
            )
        return new_dates

    # Update

    def mark_processed(self, date_str: str) -> None:
        """Mark one date as successfully transformed to silver."""
        dates = self.processed_dates
        dates.add(date_str)
        self._data["processed_dates"] = sorted(dates)

    def save(self) -> None:
        """Persist state to GCS. Call after successful Parquet write."""
        self._data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._data["entity"]       = self.entity

        blob = self._bucket.blob(self._blob_name)
        blob.upload_from_string(
            json.dumps(self._data, indent=2),
            content_type="application/json",
        )
        logger.info(
            "[silver_state/%s] Saved → gs://%s/%s (%d dates recorded).",
            self.entity, self.gcs_bucket, self._blob_name,
            len(self._data.get("processed_dates", [])),
        )

    def reset(self) -> None:
        """Reset state → all dates will be re-transformed on next run."""
        self._data = {"entity": self.entity, "processed_dates": []}
        self.save()
        logger.warning(
            "[silver_state/%s] State RESET. All bronze dates will be re-transformed.",
            self.entity,
        )

    def summary(self) -> dict:
        return {
            "entity":            self.entity,
            "is_first_run":      self.is_first_run,
            "dates_processed":   len(self.processed_dates),
            "last_updated":      self._data.get("last_updated", "never"),
            "state_blob":        f"gs://{self.gcs_bucket}/{self._blob_name}",
        }