"""
UPDATED: upload_for_directory() uses the gcs_prefix from SFTPDirectory config
instead of building the path from a hardcoded domain string.

Bronze paths now reflect the directory they came from:
  /inbound/  - gs://bucket/bronze/inbound/policies/year=2026/month=05/day=14/
  /reports/  - gs://bucket/bronze/reports/premiums/year=2026/month=05/day=14/
"""
from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path
from typing import Optional

from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError

from pipeline.config import settings
from pipeline.directory_config import SFTPDirectory

logger = logging.getLogger(__name__)

MAX_RETRIES  = 3
BACKOFF_BASE = 2  # seconds


class GCSUploader:

    def __init__(self):
        kwargs = {"project": settings.GCS_PROJECT}
        if settings.GCS_SA_KEY_PATH:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                str(settings.GCS_SA_KEY_PATH))
            kwargs["credentials"] = creds
        self.client = storage.Client(**kwargs)
        self.bucket = self.client.bucket(settings.GCS_BUCKET)

    def upload_file(
        self,
        local_path: Path,
        blob_name: str,
        metadata: Optional[dict] = None,
    ) -> str:
        """Upload with retry. Returns gs:// URI."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                blob = self.bucket.blob(blob_name)
                if metadata:
                    blob.metadata = metadata
                blob.upload_from_filename(str(local_path))
                uri = f"gs://{settings.GCS_BUCKET}/{blob_name}"
                logger.info("Uploaded: %s (attempt %d)", uri, attempt)
                return uri
            except GoogleAPICallError as exc:
                if attempt == MAX_RETRIES:
                    raise
                wait = BACKOFF_BASE ** attempt
                logger.warning("Upload failed (%s). Retrying in %ds...", exc, wait)
                time.sleep(wait)

    def upload_for_directory(
        self,
        sftp_dir: SFTPDirectory,
        valid_csv_paths: list[Path],
        business_date: date,
    ) -> dict[str, str]:
        """
        Upload validated CSV files to GCS using the gcs_prefix
        from the SFTPDirectory config.

        Returns {filename: gcs_uri} for every uploaded file.

        GCS path pattern:
          {sftp_dir.gcs_prefix}/{entity}/year=YYYY/month=MM/day=DD/{filename}

        Examples:
          bronze/inbound/policies/year=2026/month=05/day=14/policies_20260514.csv
          bronze/reports/premiums/year=2026/month=05/day=14/premiums_20260514.csv
        """
        uris: dict[str, str] = {}

        for csv_path in valid_csv_paths:
            # Determine entity from filename prefix
            entity = self._infer_entity(csv_path.name, sftp_dir)
            if not entity:
                logger.warning(
                    "Could not determine entity for file %s in dir %s — skipping.",
                    csv_path.name, sftp_dir.dir_id,
                )
                continue

            prefix   = sftp_dir.gcs_entity_prefix(entity, business_date)
            blob_name = f"{prefix}{csv_path.name}"

            uri = self.upload_file(
                csv_path,
                blob_name,
                metadata={
                    "source":        "sftp_ingestion",
                    "dir_id":        sftp_dir.dir_id,
                    "entity":        entity,
                    "business_date": business_date.isoformat(),
                },
            )
            uris[csv_path.name] = uri

        return uris

    def upload_dead_letter(
        self,
        local_path: Path,
        sftp_dir_id: str,
        entity: str,
        business_date: date,
    ) -> str:
        """Route a failed/invalid file to the dead-letter prefix."""
        blob_name = (
            f"dead_letter/{sftp_dir_id}/{entity}/"
            f"{business_date.year}/{business_date.month:02d}/{business_date.day:02d}/"
            f"{local_path.name}"
        )
        return self.upload_file(local_path, blob_name)

    @staticmethod
    def _infer_entity(filename: str, sftp_dir: SFTPDirectory) -> Optional[str]:
        """Match filename to an entity using the directory's FileEntry list."""
        for fe in sftp_dir.files:
            if filename.lower().startswith(fe.entity):
                return fe.entity
        return None