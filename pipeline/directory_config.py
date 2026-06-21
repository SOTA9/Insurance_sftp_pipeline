"""
Loads manifests/sftp_directories.yaml into typed SFTPDirectory objects.
Used by: sftp_client, file_arrival_checker, pgp_handler, gcs_uploader,
         bronze_to_silver, silver_to_gold, DAG, and all tests.

To add a third directory: add one YAML block. Nothing here changes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class FileEntry:
    entity:   str    # "policies" — used in GCS path, schema lookup, transform key
    pattern:  str    # "policies_%Y%m%d.csv.gpg"
    checksum: str    # "policies_%Y%m%d.csv.sha256"  or ""
    schema:   str    # Pandera schema key

    def filename(self, business_date: date) -> str:
        return business_date.strftime(self.pattern)

    def checksum_filename(self, business_date: date) -> Optional[str]:
        return business_date.strftime(self.checksum) if self.checksum else None


@dataclass
class SFTPDirectory:
    dir_id:       str
    label:        str
    remote_dir:   str
    use_pgp:      bool
    use_checksum: bool
    gcs_prefix:   str
    state_key:    str
    files:        list[FileEntry] = field(default_factory=list)

    def expected_data_files(self, business_date: date) -> list[str]:
        return [f.filename(business_date) for f in self.files]

    def expected_checksum_files(self, business_date: date) -> list[str]:
        if not self.use_checksum:
            return []
        return [f.checksum_filename(business_date) for f in self.files
                if f.checksum_filename(business_date)]

    def all_expected_files(self, business_date: date) -> list[str]:
        return (self.expected_data_files(business_date)
                + self.expected_checksum_files(business_date))

    def gcs_entity_prefix(self, entity: str, business_date: date) -> str:
        """
        Full GCS prefix for one entity on one date.
        e.g. "bronze/inbound/policies/year=2026/month=05/day=14/"
        """
        return (
            f"{self.gcs_prefix}/{entity}/"
            f"year={business_date.year}/"
            f"month={business_date.month:02d}/"
            f"day={business_date.day:02d}/"
        )

    @property
    def entities(self) -> list[str]:
        return [f.entity for f in self.files]


class DirectoryConfig:
    def __init__(self, config_path: Path):
        self._dirs: list[SFTPDirectory] = []
        self._load(config_path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            raise FileNotFoundError(f"sftp_directories.yaml not found: {path}")
        raw = yaml.safe_load(path.read_text())
        for block in raw.get("sftp_directories", []):
            files = [FileEntry(**f) for f in block.pop("files", [])]
            self._dirs.append(SFTPDirectory(**block, files=files))

    def all(self) -> list[SFTPDirectory]:
        return list(self._dirs)

    def get(self, dir_id: str) -> SFTPDirectory:
        for d in self._dirs:
            if d.dir_id == dir_id:
                return d
        raise KeyError(f"Unknown dir_id: {dir_id!r}. Available: {[d.dir_id for d in self._dirs]}")

    def all_entities(self) -> list[str]:
        seen, result = set(), []
        for d in self._dirs:
            for e in d.entities:
                if e not in seen:
                    seen.add(e)
                    result.append(e)
        return result

    def __len__(self) -> int:
        return len(self._dirs)


# Singleton — import this everywhere
_default_path = Path(__file__).parent.parent / "manifests" / "sftp_directories.yaml"
directory_config = DirectoryConfig(_default_path)