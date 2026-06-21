#   1. FileArrivalChecker.check() no longer takes a manifest_path and
#      a list of strings. It now takes an SFTPDirectory object and a
#      business_date and resolves expected filenames from YAML patterns.
#      ALL tests updated to reflect this new signature.
#
#   2. No more JSON manifest fixture — expected filenames come from the
#      SFTPDirectory.all_expected_files(business_date) method which
#      reads the YAML patterns at runtime.
#
#   3. Two separate test classes for the two directories:
#        TestInboundArrivalCheck  — /inbound/ with PGP + checksums
#        TestReportsArrivalCheck  — /reports/ plain CSV, no checksums
#
#   4. TestArrivalReport tests kept and extended with dir_id field.
#
#   5. TestMissingFilesRaise — confirms RuntimeError raised per-directory
#      when required files are absent from that directory's listing.
#
#   6. TestUnexpectedFilesIgnored — extra files on SFTP are logged as
#      unexpected but do NOT fail the check (same as original).

from __future__ import annotations

from datetime import date

import pytest

from pipeline.file_arrival_checker import ArrivalReport, FileArrivalChecker

BUSINESS_DATE = date(2026, 5, 14)
DATE_STR      = "20260514"


# Helpers

def inbound_expected(bd: date = BUSINESS_DATE) -> list[str]:
    """Files expected in /inbound/ for a given date."""
    ds = bd.strftime("%Y%m%d")
    return [
        f"policies_{ds}.csv.gpg",
        f"policies_{ds}.csv.sha256",
        f"claims_{ds}.csv.gpg",
        f"claims_{ds}.csv.sha256",
    ]


def reports_expected(bd: date = BUSINESS_DATE) -> list[str]:
    """Files expected in /reports/ for a given date."""
    ds = bd.strftime("%Y%m%d")
    return [
        f"premiums_{ds}.csv",
        f"reinsurance_{ds}.csv",
    ]

# TestInboundArrivalCheck — /inbound/ (PGP encrypted + checksums)

class TestInboundArrivalCheck:

    def test_all_expected_files_present(self, sftp_dir_inbound):
        """
        When all four expected files (2 GPG + 2 SHA256) are on the SFTP
        the check must pass and report no missing files.
        """
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_inbound,
            business_date=BUSINESS_DATE,
            actual_files=inbound_expected(),
        )
        assert report.is_complete
        assert len(report.missing) == 0

    def test_report_has_correct_dir_id(self, sftp_dir_inbound):
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_inbound,
            business_date=BUSINESS_DATE,
            actual_files=inbound_expected(),
        )
        assert report.dir_id == "inbound"

    def test_report_expected_matches_yaml_patterns(self, sftp_dir_inbound):
        """
        ArrivalReport.expected must be the set of filenames resolved
        from sftp_directories.yaml patterns for the given business_date.
        """
        checker  = FileArrivalChecker()
        report   = checker.check(
            sftp_dir=sftp_dir_inbound,
            business_date=BUSINESS_DATE,
            actual_files=inbound_expected(),
        )
        assert f"policies_{DATE_STR}.csv.gpg"    in report.expected
        assert f"policies_{DATE_STR}.csv.sha256" in report.expected
        assert f"claims_{DATE_STR}.csv.gpg"      in report.expected
        assert f"claims_{DATE_STR}.csv.sha256"   in report.expected

    def test_missing_gpg_file_raises(self, sftp_dir_inbound):
        """
        If the .gpg data file is missing the check must raise RuntimeError.
        """
        actual = [
            f"policies_{DATE_STR}.csv.sha256",
            f"claims_{DATE_STR}.csv.gpg",
            f"claims_{DATE_STR}.csv.sha256",
            # policies GPG missing
        ]
        checker = FileArrivalChecker()
        with pytest.raises(RuntimeError, match="Arrival check FAILED"):
            checker.check(
                sftp_dir=sftp_dir_inbound,
                business_date=BUSINESS_DATE,
                actual_files=actual,
            )

    def test_missing_checksum_sidecar_raises(self, sftp_dir_inbound):
        """
        If the .sha256 sidecar is missing for an inbound file the check
        must raise — we cannot verify integrity without it.
        """
        actual = [
            f"policies_{DATE_STR}.csv.gpg",
            # policies sha256 missing
            f"claims_{DATE_STR}.csv.gpg",
            f"claims_{DATE_STR}.csv.sha256",
        ]
        checker = FileArrivalChecker()
        with pytest.raises(RuntimeError, match="Arrival check FAILED"):
            checker.check(
                sftp_dir=sftp_dir_inbound,
                business_date=BUSINESS_DATE,
                actual_files=actual,
            )

    def test_all_files_missing_raises(self, sftp_dir_inbound):
        checker = FileArrivalChecker()
        with pytest.raises(RuntimeError, match="Arrival check FAILED"):
            checker.check(
                sftp_dir=sftp_dir_inbound,
                business_date=BUSINESS_DATE,
                actual_files=[],
            )

    def test_unexpected_extra_files_do_not_fail(self, sftp_dir_inbound):
        """
        Extra files on the SFTP (e.g. old archived files) must be reported
        as unexpected but must NOT cause the arrival check to fail.
        """
        actual = inbound_expected() + [
            "old_backup_20260501.csv.gpg",
            "manual_upload.txt",
        ]
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_inbound,
            business_date=BUSINESS_DATE,
            actual_files=actual,
        )
        assert report.is_complete
        assert "old_backup_20260501.csv.gpg" in report.unexpected
        assert "manual_upload.txt"           in report.unexpected

    def test_different_business_date_resolves_different_filenames(self, sftp_dir_inbound):
        """
        Filenames must be resolved from the actual business_date, not today.
        Run check for 2026-05-15 — expects files with 20260515 suffix.
        """
        other_date = date(2026, 5, 15)
        expected_15 = [
            "policies_20260515.csv.gpg",
            "policies_20260515.csv.sha256",
            "claims_20260515.csv.gpg",
            "claims_20260515.csv.sha256",
        ]
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_inbound,
            business_date=other_date,
            actual_files=expected_15,
        )
        assert report.is_complete
        assert report.business_date == other_date


# TestReportsArrivalCheck — /reports/ (plain CSV, no checksums)

class TestReportsArrivalCheck:

    def test_all_expected_files_present(self, sftp_dir_reports):
        """
        /reports/ expects only 2 plain CSVs — no .sha256 sidecars.
        """
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=reports_expected(),
        )
        assert report.is_complete
        assert len(report.missing) == 0

    def test_report_has_correct_dir_id(self, sftp_dir_reports):
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=reports_expected(),
        )
        assert report.dir_id == "reports"

    def test_expected_contains_no_sha256_files(self, sftp_dir_reports):
        """
        /reports/ has use_checksum=False so expected set must NOT
        contain any .sha256 filenames.
        """
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=reports_expected(),
        )
        sha_files = [f for f in report.expected if f.endswith(".sha256")]
        assert sha_files == [], (
            f"/reports/ expected should have no .sha256 files but got: {sha_files}"
        )

    def test_expected_contains_no_gpg_files(self, sftp_dir_reports):
        """
        /reports/ has use_pgp=False so expected set must NOT contain .gpg files.
        """
        checker  = FileArrivalChecker()
        report   = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=reports_expected(),
        )
        gpg_files = [f for f in report.expected if f.endswith(".gpg")]
        assert gpg_files == []

    def test_missing_premiums_raises(self, sftp_dir_reports):
        actual = [f"reinsurance_{DATE_STR}.csv"]  # premiums missing
        checker = FileArrivalChecker()
        with pytest.raises(RuntimeError, match="Arrival check FAILED"):
            checker.check(
                sftp_dir=sftp_dir_reports,
                business_date=BUSINESS_DATE,
                actual_files=actual,
            )

    def test_missing_reinsurance_raises(self, sftp_dir_reports):
        actual = [f"premiums_{DATE_STR}.csv"]  # reinsurance missing
        checker = FileArrivalChecker()
        with pytest.raises(RuntimeError, match="Arrival check FAILED"):
            checker.check(
                sftp_dir=sftp_dir_reports,
                business_date=BUSINESS_DATE,
                actual_files=actual,
            )

    def test_unexpected_files_in_reports_ignored(self, sftp_dir_reports):
        actual = reports_expected() + ["random_extra_file.csv"]
        checker = FileArrivalChecker()
        report  = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=actual,
        )
        assert report.is_complete
        assert "random_extra_file.csv" in report.unexpected


# TestArrivalReport — unit tests for ArrivalReport dataclass

class TestArrivalReport:

    def test_missing_computed_correctly(self, sftp_dir_inbound):
        expected = set(inbound_expected())
        actual   = {f"policies_{DATE_STR}.csv.gpg",
                    f"policies_{DATE_STR}.csv.sha256"}  # claims files missing

        report = ArrivalReport(
            dir_id="inbound",
            business_date=BUSINESS_DATE,
            expected=expected,
            actual=actual,
        )
        assert f"claims_{DATE_STR}.csv.gpg"    in report.missing
        assert f"claims_{DATE_STR}.csv.sha256" in report.missing

    def test_unexpected_computed_correctly(self, sftp_dir_inbound):
        expected = set(inbound_expected())
        actual   = set(inbound_expected()) | {"surprise_file.csv"}

        report = ArrivalReport(
            dir_id="inbound",
            business_date=BUSINESS_DATE,
            expected=expected,
            actual=actual,
        )
        assert "surprise_file.csv" in report.unexpected

    def test_is_complete_true_when_no_missing(self, sftp_dir_inbound):
        expected = set(inbound_expected())
        report   = ArrivalReport(
            dir_id="inbound",
            business_date=BUSINESS_DATE,
            expected=expected,
            actual=expected,
        )
        assert report.is_complete is True

    def test_is_complete_false_when_files_missing(self, sftp_dir_inbound):
        expected = set(inbound_expected())
        actual   = {f"policies_{DATE_STR}.csv.gpg"}  # only one file

        report = ArrivalReport(
            dir_id="inbound",
            business_date=BUSINESS_DATE,
            expected=expected,
            actual=actual,
        )
        assert report.is_complete is False

    def test_summary_contains_dir_id(self, sftp_dir_reports):
        report = ArrivalReport(
            dir_id="reports",
            business_date=BUSINESS_DATE,
            expected=set(reports_expected()),
            actual=set(reports_expected()),
        )
        assert "reports" in report.summary()

    def test_summary_contains_counts(self, sftp_dir_inbound):
        report = ArrivalReport(
            dir_id="inbound",
            business_date=BUSINESS_DATE,
            expected=set(inbound_expected()),
            actual=set(inbound_expected()),
        )
        summary = report.summary()
        assert "expected=4" in summary
        assert "actual=4"   in summary
        assert "missing=0"  in summary


# TestBothDirectoriesChecked — NEW: verifies two independent checks work

class TestBothDirectoriesChecked:

    def test_inbound_and_reports_checked_independently(
        self, sftp_dir_inbound, sftp_dir_reports
    ):
        """
        Each directory's checker runs independently.
        Failure in one must not affect the other.
        """
        checker = FileArrivalChecker()

        # /inbound/ check passes
        inbound_report = checker.check(
            sftp_dir=sftp_dir_inbound,
            business_date=BUSINESS_DATE,
            actual_files=inbound_expected(),
        )

        # /reports/ check passes
        reports_report = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=reports_expected(),
        )

        assert inbound_report.is_complete
        assert reports_report.is_complete
        assert inbound_report.dir_id != reports_report.dir_id

    def test_inbound_failure_does_not_prevent_reports_check(
        self, sftp_dir_inbound, sftp_dir_reports
    ):
        """
        If /inbound/ check raises, /reports/ check can still run.
        In the DAG these are in separate TaskGroups so they are independent.
        """
        checker = FileArrivalChecker()

        # /inbound/ fails — missing all files
        with pytest.raises(RuntimeError):
            checker.check(
                sftp_dir=sftp_dir_inbound,
                business_date=BUSINESS_DATE,
                actual_files=[],
            )

        # /reports/ check runs independently and passes
        reports_report = checker.check(
            sftp_dir=sftp_dir_reports,
            business_date=BUSINESS_DATE,
            actual_files=reports_expected(),
        )
        assert reports_report.is_complete