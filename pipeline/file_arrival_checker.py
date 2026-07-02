from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Set

from pipeline.config import settings
from pipeline.directory_config import SFTPDirectory

logger = logging.getLogger(__name__)

@dataclass
class ArrivalReport:
    dir_id:        str
    business_date: date
    expected:      Set[str]
    actual:        Set[str]
    missing:       Set[str] = field(init=False)
    unexpected:    Set[str] = field(init=False)

    def __post_init__(self):
        self.missing    = self.expected - self.actual
        self.unexpected = self.actual   - self.expected

    @property
    def is_complete(self) -> bool:
        return len(self.missing) <= settings.MAX_MISSING_FILES_ALLOWED

    def summary(self) -> str:
        return (
            f"[{self.dir_id}] date={self.business_date} "
            f"expected={len(self.expected)} actual={len(self.actual)} "
            f"missing={len(self.missing)} unexpected={len(self.unexpected)}"
        )


class FileArrivalChecker:
    """
    Compares expected filenames (resolved from SFTPDirectory patterns)
    against the actual files present on the SFTP server.

    Called once per SFTPDirectory per DAG run.
    """

    def check(
        self,
        sftp_dir: SFTPDirectory,
        business_date: date,
        actual_files: list[str],
    ) -> ArrivalReport:
        """
        sftp_dir:     the directory config from directory_config.py
        business_date: the Airflow logical date (context["ds"])
        actual_files:  raw output of sftp_client.list_remote_files(remote_dir)

        Returns ArrivalReport. Raises RuntimeError if required files are missing.
        """
        # Resolve expected filenames from YAML patterns
        expected = set(sftp_dir.all_expected_files(business_date))
        actual   = set(actual_files)

        report = ArrivalReport(
            dir_id=sftp_dir.dir_id,
            business_date=business_date,
            expected=expected,
            actual=actual,
        )

        logger.info("Arrival check: %s", report.summary())

        if report.missing:
            logger.warning("[%s] MISSING FILES: %s",
                           sftp_dir.dir_id, sorted(report.missing))
        if report.unexpected:
            logger.info("[%s] UNEXPECTED FILES (will be ignored): %s",
                        sftp_dir.dir_id, sorted(report.unexpected))

        if not report.is_complete:
            raise RuntimeError(
                f"[{sftp_dir.dir_id}] Arrival check FAILED. "
                f"Missing: {sorted(report.missing)}"
            )

        return report