from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_STATE_ROOT = "_state"


class SFTPState:
    """
    Manages incremental load state for one SFTP directory.
    """

    def __init__(
        self,
        gcs_bucket: str,
        state_key: str,        # e.g. "sftp/inbound" — from sftp_directories.yaml
        gcs_project: str,
        sa_key_path: Optional[Path] = None,
    ):
        self.gcs_bucket = gcs_bucket
        self.state_key  = state_key
        self._blob_name = f"{_STATE_ROOT}/{state_key}/processed_files.json"

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
        """
        Load state from GCS.
        Returns empty state on first run (file does not exist yet).
        """
        blob = self._bucket.blob(self._blob_name)
        try:
            raw  = blob.download_as_text()
            data = json.loads(raw)
            n    = len(data.get("processed_files", {}))
            logger.info(
                "[state/%s] Loaded: %d files already processed. "
                "Last updated: %s",
                self.state_key, n,
                data.get("last_updated", "unknown"),
            )
            return data
        except Exception:
            logger.info(
                "[state/%s] No state file found — this is a FULL LOAD.",
                self.state_key,
            )
            return {"processed_files": {}}

    # Public query API

    @property
    def is_first_run(self) -> bool:
        """True when no files have been processed yet (state is empty)."""
        return len(self._data.get("processed_files", {})) == 0

    @property
    def processed_set(self) -> set[str]:
        """Set of all filenames already in state."""
        return set(self._data.get("processed_files", {}).keys())

    def get_new_files(self, sftp_listing: list[str]) -> list[str]:
        """
        Compare the current SFTP directory listing against state.
        Returns only filenames NOT yet processed.

        First run  : returns all files in sftp_listing.
        Later runs : returns only the delta (new files).

        sftp_listing: raw output of sftp.list_remote_files(remote_dir)
        """
        processed = self.processed_set
        new_files = [f for f in sftp_listing if f not in processed]

        if self.is_first_run:
            logger.info(
                "[state/%s] FULL LOAD: %d files on SFTP → processing all.",
                self.state_key, len(new_files),
            )
        else:
            logger.info(
                "[state/%s] INCREMENTAL: %d total on SFTP | %d already seen | %d new.",
                self.state_key,
                len(sftp_listing),
                len(processed),
                len(new_files),
            )
            if not new_files:
                logger.info(
                    "[state/%s] Nothing new — all SFTP files already processed.",
                    self.state_key,
                )

        return new_files

    def is_processed(self, filename: str) -> bool:
        return filename in self._data.get("processed_files", {})

    # Public update API

    def mark_processed(
        self,
        filename: str,
        business_date: str,     # ISO format "2026-05-14"
        gcs_bronze_uri: str = "",
    ) -> None:
        """
        Record one file as successfully processed.
        Call AFTER the file has been uploaded to GCS bronze.
        """
        if "processed_files" not in self._data:
            self._data["processed_files"] = {}

        self._data["processed_files"][filename] = {
            "processed_at":   datetime.now(timezone.utc).isoformat(),
            "business_date":  business_date,
            "gcs_bronze_uri": gcs_bronze_uri,
        }

    def mark_batch(
        self,
        filenames: list[str],
        business_date: str,
        gcs_uris: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Mark multiple files processed at once.
        gcs_uris: optional dict {filename: gcs_uri}
        """
        for fn in filenames:
            self.mark_processed(
                filename=fn,
                business_date=business_date,
                gcs_bronze_uri=(gcs_uris or {}).get(fn, ""),
            )

    def save(self) -> None:
        """
        Persist the current state to GCS.
        MUST be called after every successful batch upload.
        """
        self._data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._data["state_key"]    = self.state_key

        blob = self._bucket.blob(self._blob_name)
        blob.upload_from_string(
            json.dumps(self._data, indent=2, ensure_ascii=False),
            content_type="application/json",
        )
        total = len(self._data.get("processed_files", {}))
        logger.info(
            "[state/%s] Saved -> gs://%s/%s (%d total files recorded).",
            self.state_key, self.gcs_bucket, self._blob_name, total,
        )

    def reset(self) -> None:
        """
        Clear state -> triggers a full reload on the next DAG run.
        Use when you need to reprocess all files (e.g. bronze partition deleted,
        schema fix that requires re-landing all files).
        """
        self._data = {"processed_files": {}}
        self.save()
        logger.warning(
            "[state/%s] State RESET. Next run will be a FULL LOAD.",
            self.state_key,
        )

    def summary(self) -> dict:
        return {
            "state_key":       self.state_key,
            "is_first_run":    self.is_first_run,
            "files_processed": len(self._data.get("processed_files", {})),
            "last_updated":    self._data.get("last_updated", "never"),
            "state_blob":      f"gs://{self.gcs_bucket}/{self._blob_name}",
        }

    def __repr__(self) -> str:
        return (
            f"SFTPState(state_key={self.state_key!r}, "
            f"processed={len(self.processed_set)}, "
            f"first_run={self.is_first_run})"
        )