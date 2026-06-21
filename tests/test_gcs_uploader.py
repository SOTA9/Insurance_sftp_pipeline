#   1. upload_batch() tests REMOVED — that method no longer exists.
#      It is replaced by upload_for_directory() which takes an SFTPDirectory
#      and builds GCS paths from the directory's gcs_prefix config.
#
#   2. build_gcs_prefix() tests REMOVED — that standalone function is gone.
#      GCS prefix is now built inside GCSUploader.upload_for_directory()
#      using SFTPDirectory.gcs_entity_prefix().
#
#   3. New class TestUploadForDirectory — main new test class covering:
#        - /inbound/ files go to bronze/inbound/{entity}/year=.../...
#        - /reports/ files go to bronze/reports/{entity}/year=.../...
#        - entity is inferred from filename using sftp_dir.files entries
#        - metadata tags include dir_id from sftp_dir
#        - returns {filename: uri} dict
#
#   4. TestUploadDeadLetter updated — dead_letter paths now include dir_id:
#      dead_letter/{dir_id}/{entity}/year=.../...
#
#   5. TestInferEntity — new class testing _infer_entity() which maps
#      filenames to entity names using SFTPDirectory.files entries.
#
#   6. TestRetryLogic unchanged — retry on GoogleAPICallError still works
#      the same way through upload_file() which is unchanged.
#
#   7. All GCS client calls remain mocked — no real GCP credentials needed.

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from google.api_core.exceptions import GoogleAPICallError

from pipeline.gcs_uploader import GCSUploader

BUSINESS_DATE = date(2026, 5, 14)
DATE_STR      = "20260514"


# Fixtures

@pytest.fixture
def mock_gcs(mock_settings):
    """Mocked GCS client, bucket, and blob. Returns (uploader, bucket, blob)."""
    mock_blob   = MagicMock()
    mock_bucket = MagicMock()
    mock_client = MagicMock()

    mock_bucket.blob.return_value              = mock_blob
    mock_client.bucket.return_value            = mock_bucket
    mock_blob.upload_from_filename.return_value = None

    with patch("google.cloud.storage.Client", return_value=mock_client):
        uploader = GCSUploader()

    uploader.client = mock_client
    uploader.bucket = mock_bucket
    return uploader, mock_bucket, mock_blob


@pytest.fixture
def policies_csv(tmp_path) -> Path:
    p = tmp_path / f"policies_{DATE_STR}.csv"
    p.write_text("policy_id,premium\nPOL-20260001,1500\n")
    return p


@pytest.fixture
def claims_csv(tmp_path) -> Path:
    p = tmp_path / f"claims_{DATE_STR}.csv"
    p.write_text("claim_id,amount\nCLM-20260001,5000\n")
    return p


@pytest.fixture
def premiums_csv(tmp_path) -> Path:
    p = tmp_path / f"premiums_{DATE_STR}.csv"
    p.write_text("premium_id,amount_due\nPRM-20260001,125\n")
    return p


@pytest.fixture
def reinsurance_csv(tmp_path) -> Path:
    p = tmp_path / f"reinsurance_{DATE_STR}.csv"
    p.write_text("ri_record_id,ceded_premium\nRI-20260001,450\n")
    return p


# TestUploadFile — low-level upload, unchanged logic

class TestUploadFile:

    def test_returns_gs_uri(self, mock_gcs, policies_csv, mock_settings):
        uploader, _, _ = mock_gcs
        uri = uploader.upload_file(
            policies_csv,
            "bronze/inbound/policies/year=2026/month=05/day=14/policies_20260514.csv",
        )
        assert uri.startswith("gs://")
        assert mock_settings.GCS_BUCKET in uri

    def test_blob_upload_called_with_correct_path(self, mock_gcs, policies_csv):
        uploader, _, mock_blob = mock_gcs
        uploader.upload_file(policies_csv, "some/prefix/policies.csv")
        mock_blob.upload_from_filename.assert_called_once_with(str(policies_csv))

    def test_metadata_set_on_blob(self, mock_gcs, policies_csv):
        uploader, _, mock_blob = mock_gcs
        meta = {
            "source":        "sftp_ingestion",
            "dir_id":        "inbound",
            "entity":        "policies",
            "business_date": "2026-05-14",
        }
        uploader.upload_file(policies_csv, "prefix/policies.csv", metadata=meta)
        assert mock_blob.metadata == meta

    def test_retry_on_transient_api_error(self, mock_gcs, policies_csv):
        uploader, _, mock_blob = mock_gcs
        mock_blob.upload_from_filename.side_effect = [
            GoogleAPICallError("transient"),
            GoogleAPICallError("transient"),
            None,   # success on third attempt
        ]
        with patch("time.sleep"):
            uri = uploader.upload_file(policies_csv, "prefix/file.csv")
        assert uri.startswith("gs://")
        assert mock_blob.upload_from_filename.call_count == 3

    def test_raises_after_max_retries_exceeded(self, mock_gcs, policies_csv):
        uploader, _, mock_blob = mock_gcs
        mock_blob.upload_from_filename.side_effect = GoogleAPICallError("persistent")
        with patch("time.sleep"):
            with pytest.raises(GoogleAPICallError):
                uploader.upload_file(policies_csv, "prefix/file.csv")
        assert mock_blob.upload_from_filename.call_count == 3


# TestUploadForDirectory — NEW main entry point replacing upload_batch

class TestUploadForDirectory:

    def test_inbound_policies_go_to_bronze_inbound_prefix(
        self, mock_gcs, policies_csv, sftp_dir_inbound, mock_settings
    ):
        """
        /inbound/ policies file must land at:
        gs://bucket/bronze/inbound/policies/year=2026/month=05/day=14/
        """
        uploader, mock_bucket, _ = mock_gcs

        uris = uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv],
            business_date=BUSINESS_DATE,
        )

        assert len(uris) == 1
        blob_name = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("bronze/inbound/policies/")
        assert "year=2026" in blob_name
        assert "month=05"  in blob_name
        assert "day=14"    in blob_name
        assert blob_name.endswith(f"policies_{DATE_STR}.csv")

    def test_inbound_claims_go_to_bronze_inbound_prefix(
        self, mock_gcs, claims_csv, sftp_dir_inbound
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[claims_csv],
            business_date=BUSINESS_DATE,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("bronze/inbound/claims/")

    def test_reports_premiums_go_to_bronze_reports_prefix(
        self, mock_gcs, premiums_csv, sftp_dir_reports
    ):
        """
        /reports/ premiums file must land at:
        gs://bucket/bronze/reports/premiums/year=2026/month=05/day=14/
        NOT bronze/inbound/...
        """
        uploader, mock_bucket, _ = mock_gcs

        uris = uploader.upload_for_directory(
            sftp_dir=sftp_dir_reports,
            valid_csv_paths=[premiums_csv],
            business_date=BUSINESS_DATE,
        )

        assert len(uris) == 1
        blob_name = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("bronze/reports/premiums/")
        assert "year=2026" in blob_name
        assert "month=05"  in blob_name
        assert "day=14"    in blob_name

    def test_reports_reinsurance_go_to_bronze_reports_prefix(
        self, mock_gcs, reinsurance_csv, sftp_dir_reports
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_reports,
            valid_csv_paths=[reinsurance_csv],
            business_date=BUSINESS_DATE,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("bronze/reports/reinsurance/")

    def test_inbound_and_reports_have_different_prefixes(
        self, mock_gcs, policies_csv, premiums_csv,
        sftp_dir_inbound, sftp_dir_reports
    ):
        """
        Critical: /inbound/ and /reports/ must write to DIFFERENT GCS paths.
        bronze/inbound/... vs bronze/reports/...
        """
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv],
            business_date=BUSINESS_DATE,
        )
        inbound_blob = mock_bucket.blob.call_args_list[-1][0][0]

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_reports,
            valid_csv_paths=[premiums_csv],
            business_date=BUSINESS_DATE,
        )
        reports_blob = mock_bucket.blob.call_args_list[-1][0][0]

        assert inbound_blob.startswith("bronze/inbound/")
        assert reports_blob.startswith("bronze/reports/")
        assert inbound_blob != reports_blob

    def test_returns_filename_to_uri_dict(
        self, mock_gcs, policies_csv, sftp_dir_inbound, mock_settings
    ):
        """
        upload_for_directory returns {filename: gcs_uri} dict so the
        DAG can map each filename to its GCS location for state commit.
        """
        uploader, _, _ = mock_gcs

        uris = uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv],
            business_date=BUSINESS_DATE,
        )

        assert isinstance(uris, dict)
        assert f"policies_{DATE_STR}.csv" in uris
        assert uris[f"policies_{DATE_STR}.csv"].startswith(
            f"gs://{mock_settings.GCS_BUCKET}/bronze/inbound/policies/"
        )

    def test_multiple_files_returns_all_uris(
        self, mock_gcs, policies_csv, claims_csv, sftp_dir_inbound
    ):
        uploader, _, _ = mock_gcs

        uris = uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv, claims_csv],
            business_date=BUSINESS_DATE,
        )

        assert len(uris) == 2
        assert f"policies_{DATE_STR}.csv" in uris
        assert f"claims_{DATE_STR}.csv"   in uris

    def test_metadata_includes_dir_id_and_entity(
        self, mock_gcs, policies_csv, sftp_dir_inbound
    ):
        """
        GCS object metadata must include dir_id and entity so each file
        is traceable to its SFTP source directory.
        """
        uploader, _, mock_blob = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv],
            business_date=BUSINESS_DATE,
        )

        assert mock_blob.metadata is not None
        assert mock_blob.metadata["dir_id"]        == "inbound"
        assert mock_blob.metadata["entity"]        == "policies"
        assert mock_blob.metadata["business_date"] == "2026-05-14"
        assert mock_blob.metadata["source"]        == "sftp_ingestion"

    def test_reports_metadata_includes_reports_dir_id(
        self, mock_gcs, premiums_csv, sftp_dir_reports
    ):
        uploader, _, mock_blob = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_reports,
            valid_csv_paths=[premiums_csv],
            business_date=BUSINESS_DATE,
        )

        assert mock_blob.metadata["dir_id"] == "reports"
        assert mock_blob.metadata["entity"] == "premiums"

    def test_empty_file_list_returns_empty_dict(
        self, mock_gcs, sftp_dir_inbound
    ):
        uploader, mock_bucket, _ = mock_gcs

        uris = uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[],
            business_date=BUSINESS_DATE,
        )

        assert uris == {}
        mock_bucket.blob.assert_not_called()

    def test_hive_partition_path_format(
        self, mock_gcs, policies_csv, sftp_dir_inbound
    ):
        """
        GCS path must follow Hive partition format:
        year=YYYY/month=MM/day=DD/
        with zero-padded month and day.
        """
        uploader, mock_bucket, _ = mock_gcs
        bd = date(2026, 1, 5)  # January 5 — tests zero-padding

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv],
            business_date=bd,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert "year=2026"  in blob_name
        assert "month=01"   in blob_name   # zero-padded
        assert "day=05"     in blob_name   # zero-padded


# TestUploadDeadLetter — updated with dir_id in path

class TestUploadDeadLetter:

    def test_inbound_dead_letter_path_contains_dir_id(
        self, mock_gcs, policies_csv
    ):
        """
        Dead-letter path must include dir_id so ops team knows
        which SFTP directory the bad file came from.
        Pattern: dead_letter/{dir_id}/{entity}/year=.../...
        """
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_dead_letter(
            local_path=policies_csv,
            sftp_dir_id="inbound",
            entity="policies",
            business_date=BUSINESS_DATE,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("dead_letter/inbound/policies/")

    def test_reports_dead_letter_path_contains_dir_id(
        self, mock_gcs, premiums_csv
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_dead_letter(
            local_path=premiums_csv,
            sftp_dir_id="reports",
            entity="premiums",
            business_date=BUSINESS_DATE,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("dead_letter/reports/premiums/")

    def test_dead_letter_path_contains_date_partition(
        self, mock_gcs, policies_csv
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_dead_letter(
            local_path=policies_csv,
            sftp_dir_id="inbound",
            entity="policies",
            business_date=BUSINESS_DATE,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert "2026" in blob_name
        assert "05"   in blob_name
        assert "14"   in blob_name

    def test_dead_letter_contains_original_filename(
        self, mock_gcs, policies_csv
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_dead_letter(
            local_path=policies_csv,
            sftp_dir_id="inbound",
            entity="policies",
            business_date=BUSINESS_DATE,
        )

        blob_name = mock_bucket.blob.call_args[0][0]
        assert policies_csv.name in blob_name

    def test_dead_letter_returns_gs_uri(self, mock_gcs, policies_csv, mock_settings):
        uploader, _, _ = mock_gcs
        uri = uploader.upload_dead_letter(
            local_path=policies_csv,
            sftp_dir_id="inbound",
            entity="policies",
            business_date=BUSINESS_DATE,
        )
        assert uri.startswith("gs://")
        assert mock_settings.GCS_BUCKET in uri


# TestInferEntity — NEW: maps filename → entity using sftp_dir context

class TestInferEntity:

    def test_infer_policies_from_inbound(self, mock_gcs, sftp_dir_inbound):
        uploader, _, _ = mock_gcs
        result = uploader._infer_entity(f"policies_{DATE_STR}.csv", sftp_dir_inbound)
        assert result == "policies"

    def test_infer_claims_from_inbound(self, mock_gcs, sftp_dir_inbound):
        uploader, _, _ = mock_gcs
        result = uploader._infer_entity(f"claims_{DATE_STR}.csv", sftp_dir_inbound)
        assert result == "claims"

    def test_infer_premiums_from_reports(self, mock_gcs, sftp_dir_reports):
        uploader, _, _ = mock_gcs
        result = uploader._infer_entity(f"premiums_{DATE_STR}.csv", sftp_dir_reports)
        assert result == "premiums"

    def test_infer_reinsurance_from_reports(self, mock_gcs, sftp_dir_reports):
        uploader, _, _ = mock_gcs
        result = uploader._infer_entity(f"reinsurance_{DATE_STR}.csv", sftp_dir_reports)
        assert result == "reinsurance"

    def test_unknown_filename_returns_none(self, mock_gcs, sftp_dir_inbound):
        uploader, _, _ = mock_gcs
        result = uploader._infer_entity("unknown_file.csv", sftp_dir_inbound)
        assert result is None

    def test_case_insensitive_match(self, mock_gcs, sftp_dir_inbound):
        """Filename matching must be case-insensitive."""
        uploader, _, _ = mock_gcs
        result = uploader._infer_entity(f"POLICIES_{DATE_STR}.CSV", sftp_dir_inbound)
        assert result == "policies"


# TestGcsPathIsolation — ensures /inbound/ and /reports/ never mix paths

class TestGcsPathIsolation:

    def test_inbound_never_writes_to_reports_prefix(
        self, mock_gcs, policies_csv, claims_csv, sftp_dir_inbound
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv, claims_csv],
            business_date=BUSINESS_DATE,
        )

        all_blob_names = [c[0][0] for c in mock_bucket.blob.call_args_list]
        for bn in all_blob_names:
            assert not bn.startswith("bronze/reports/"), (
                f"Inbound file written to reports prefix: {bn}"
            )

    def test_reports_never_writes_to_inbound_prefix(
        self, mock_gcs, premiums_csv, reinsurance_csv, sftp_dir_reports
    ):
        uploader, mock_bucket, _ = mock_gcs

        uploader.upload_for_directory(
            sftp_dir=sftp_dir_reports,
            valid_csv_paths=[premiums_csv, reinsurance_csv],
            business_date=BUSINESS_DATE,
        )

        all_blob_names = [c[0][0] for c in mock_bucket.blob.call_args_list]
        for bn in all_blob_names:
            assert not bn.startswith("bronze/inbound/"), (
                f"Reports file written to inbound prefix: {bn}"
            )

    def test_full_run_both_directories_correct_paths(
        self, mock_gcs,
        policies_csv, claims_csv,
        premiums_csv, reinsurance_csv,
        sftp_dir_inbound, sftp_dir_reports,
    ):
        """
        Simulate a full DAG run: upload all four entities from both directories.
        Verify each file lands under its correct directory prefix.
        """
        uploader, mock_bucket, _ = mock_gcs

        # Upload /inbound/ files
        uploader.upload_for_directory(
            sftp_dir=sftp_dir_inbound,
            valid_csv_paths=[policies_csv, claims_csv],
            business_date=BUSINESS_DATE,
        )
        # Upload /reports/ files
        uploader.upload_for_directory(
            sftp_dir=sftp_dir_reports,
            valid_csv_paths=[premiums_csv, reinsurance_csv],
            business_date=BUSINESS_DATE,
        )

        all_blob_names = [c[0][0] for c in mock_bucket.blob.call_args_list]

        inbound_blobs = [b for b in all_blob_names if b.startswith("bronze/inbound/")]
        reports_blobs = [b for b in all_blob_names if b.startswith("bronze/reports/")]

        assert len(inbound_blobs) == 2, f"Expected 2 inbound blobs, got: {inbound_blobs}"
        assert len(reports_blobs) == 2, f"Expected 2 reports blobs, got: {reports_blobs}"

        inbound_entities = {b.split("/")[2] for b in inbound_blobs}
        reports_entities = {b.split("/")[2] for b in reports_blobs}

        assert inbound_entities == {"policies", "claims"}
        assert reports_entities == {"premiums", "reinsurance"}