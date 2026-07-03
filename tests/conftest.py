from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import gnupg
import pandas as pd
import pytest
import yaml

BUSINESS_DATE = date(2026, 5, 14)
DATE_STR      = "20260514"


# PGP fixtures (UNCHANGED)

@pytest.fixture(scope="session")
def gpg_home(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("gpg_home")
    d.chmod(0o700)
    return d


@pytest.fixture(scope="session")
def pgp_keypair(gpg_home) -> dict:
    passphrase = "test-passphrase-insureflow"
    gpg = gnupg.GPG(gnupghome=str(gpg_home))
    gpg.encoding = "utf-8"
    input_data = gpg.gen_key_input(
        key_type="RSA", key_length=2048,
        name_real="InsureFlow Test", name_email="test@insureflow.io",
        passphrase=passphrase, expire_date="1y",
    )
    key = gpg.gen_key(input_data)
    assert key.fingerprint, "PGP key generation failed"
    return {
        "gpg": gpg, "fingerprint": key.fingerprint,
        "passphrase": passphrase, "gpg_home": gpg_home,
    }


@pytest.fixture(scope="session")
def pgp_public_key_file(gpg_home, pgp_keypair, tmp_path_factory) -> Path:
    asc = pgp_keypair["gpg"].export_keys(pgp_keypair["fingerprint"], armor=True)
    p   = tmp_path_factory.mktemp("keys") / "public_key.asc"
    p.write_text(asc)
    return p


@pytest.fixture(scope="session")
def pgp_private_key_file(gpg_home, pgp_keypair, tmp_path_factory) -> Path:
    asc = pgp_keypair["gpg"].export_keys(
        pgp_keypair["fingerprint"], secret=True, armor=True,
        passphrase=pgp_keypair["passphrase"],
    )
    p = tmp_path_factory.mktemp("keys") / "private_key.asc"
    p.write_text(asc)
    p.chmod(0o600)
    return p


# SFTPDirectory fixtures (NEW)

@pytest.fixture(scope="session")
def sftp_dir_inbound():
    """
    SFTPDirectory config for /inbound/ — policies + claims, PGP encrypted.
    Used by all tests that need the inbound directory context.
    """
    from pipeline.directory_config import SFTPDirectory, FileEntry
    return SFTPDirectory(
        dir_id="inbound",
        label="Operational files (policies + claims)",
        remote_dir="/inbound/",
        use_pgp=True,
        use_checksum=True,
        gcs_prefix="bronze/inbound",
        state_key="sftp/inbound",
        files=[
            FileEntry(
                entity="policies",
                pattern="policies_%Y%m%d.csv.gpg",
                checksum="policies_%Y%m%d.csv.sha256",
                schema="policies",
            ),
            FileEntry(
                entity="claims",
                pattern="claims_%Y%m%d.csv.gpg",
                checksum="claims_%Y%m%d.csv.sha256",
                schema="claims",
            ),
        ],
    )


@pytest.fixture(scope="session")
def sftp_dir_reports():
    """
    SFTPDirectory config for /reports/ — premiums + reinsurance, plain CSV.
    Used by all tests that need the reports directory context.
    """
    from pipeline.directory_config import SFTPDirectory, FileEntry
    return SFTPDirectory(
        dir_id="reports",
        label="Financial reports (premiums + reinsurance)",
        remote_dir="/reports/",
        use_pgp=False,
        use_checksum=False,
        gcs_prefix="bronze/reports",
        state_key="sftp/reports",
        files=[
            FileEntry(
                entity="premiums",
                pattern="premiums_%Y%m%d.csv",
                checksum="",
                schema="premiums",
            ),
            FileEntry(
                entity="reinsurance",
                pattern="reinsurance_%Y%m%d.csv",
                checksum="",
                schema="reinsurance",
            ),
        ],
    )


# Mock settings (UPDATED)

@pytest.fixture(autouse=True)
def mock_settings(pgp_public_key_file, pgp_private_key_file, tmp_path):
    """
    Patch pipeline.config.settings with test values.
    UPDATED: SFTP_REMOTE_DIR removed, SFTP_DIRECTORIES_YAML and
    BQ_GOLD_DATASET added. ENVIRONMENT set to "dev".
    """
    m = MagicMock()

    # SFTP — no SFTP_REMOTE_DIR (removed — paths come from YAML now)
    m.SFTP_HOST          = "sftp.test.local"
    m.SFTP_PORT          = 22
    m.SFTP_USER          = "testuser"
    m.SFTP_SSH_KEY_PATH  = tmp_path / "sftp_rsa_key"
    m.SFTP_KNOWN_HOSTS   = tmp_path / "known_hosts"
    m.SFTP_DIRECTORIES_YAML = tmp_path / "sftp_directories.yaml"  # ADDED

    # PGP
    m.PGP_PUBLIC_KEY_PATH   = pgp_public_key_file
    m.PGP_PRIVATE_KEY_PATH  = pgp_private_key_file
    m.PGP_PASSPHRASE.get_secret_value.return_value = "test-passphrase-insureflow"
    m.PGP_GNUPGHOME         = tmp_path / "gnupg"

    # GCS
    m.GCS_BUCKET      = "insureflow-test-bucket"
    m.GCS_PROJECT     = "insureflow-test-project"
    m.GCS_SA_KEY_PATH = None

    # Validation
    m.CHECKSUM_EXTENSION        = ".sha256"
    m.MAX_MISSING_FILES_ALLOWED = 0

    # BigQuery
    m.BQ_DATASET      = "pipeline_audit"
    m.BQ_TABLE        = "ingestion_runs"
    m.BQ_GOLD_DATASET = "gold_insurance"   # ADDED

    # Alerting
    m.SLACK_WEBHOOK_URL = None
    m.PAGERDUTY_API_KEY = None
    m.ALERT_EMAIL       = "test@insureflow.io"

    # Environment — "dev" skips Secret Manager in entrypoint.sh
    m.ENVIRONMENT = "dev"   # ADDED

    patches = [
        patch("pipeline.config.settings",                m),
        patch("pipeline.sftp_client.settings",           m),
        patch("pipeline.pgp_handler.settings",           m),
        patch("pipeline.checksum_validator.settings",    m, create=True),
        patch("pipeline.file_arrival_checker.settings",  m),
        patch("pipeline.gcs_uploader.settings",          m),
        patch("pipeline.audit_logger.settings",          m),
        patch("pipeline.alerting.settings",              m),
        patch("pipeline.data_validator.settings",        m, create=True),
        patch("pipeline.transforms.bronze_to_silver.settings", m, create=True),
        patch("pipeline.transforms.silver_to_gold.settings",   m, create=True),
        patch("pipeline.state.settings",                 m, create=True),
    ]
    with patch("pipeline.config.get_dir_pgp_passphrase",
               return_value="test-passphrase-insureflow"):
        with patch("pipeline.config.get_dir_pgp_key_path",
                   return_value=pgp_private_key_file):
            for p in patches:
                p.start()
            yield m
            for p in patches:
                p.stop()


# Sample data helpers (UPDATED)

def _make_policies_df() -> pd.DataFrame:
    return pd.DataFrame({
        "policy_id":         ["POL-20260001", "POL-20260002"],
        "insured_name":      ["Alice Martin", "Bob Dupont"],
        "policy_start_date": ["2024-01-01",   "2023-06-15"],
        "policy_end_date":   ["2025-01-01",   "2024-06-15"],
        "premium_amount":    [1500.0,          2300.0],
        "coverage_type":     ["LIFE",          "HEALTH"],
        "risk_score":        [0.25,            0.65],
        "broker_id":         ["BRK-001",       None],
        "status":            ["ACTIVE",        "ACTIVE"],
    })


def _make_claims_df() -> pd.DataFrame:
    return pd.DataFrame({
        "claim_id":           ["CLM-20260001", "CLM-20260002"],
        "policy_id":          ["POL-20260001", "POL-20260002"],
        "claimant_name":      ["Alice Martin", "Bob Dupont"],
        "incident_date":      ["2025-03-10",   "2025-07-22"],
        "reported_date":      ["2025-03-15",   "2025-07-25"],
        "claim_type":         ["MEDICAL",      "ACCIDENT"],
        "claimed_amount":     [5000.0,         12000.0],
        "approved_amount":    [4500.0,         None],
        "status":             ["APPROVED",     "IN_REVIEW"],
        "adjuster_id":        ["ADJ-001",      "ADJ-002"],
        "deductible_applied": [500.0,          None],
        "fraud_flag":         [False,          False],
    })


def _make_premiums_df() -> pd.DataFrame:
    return pd.DataFrame({
        "premium_id":       ["PRM-20260001", "PRM-20260002"],
        "policy_id":        ["POL-20260001", "POL-20260002"],
        "payment_date":     ["2026-01-01",   None],
        "due_date":         ["2026-01-01",   "2026-02-01"],
        "amount_due":       [125.0,          191.67],
        "amount_paid":      [125.0,          None],
        "payment_method":   ["BANK_TRANSFER", None],
        "currency":         ["EUR",           "EUR"],
        "instalment_number":[1,              2],
        "instalment_total": [12,             12],
        "status":           ["PAID",         "PENDING"],
        "late_fee":         [0.0,            None],
    })


def _make_reinsurance_df() -> pd.DataFrame:
    return pd.DataFrame({
        "ri_record_id":       ["RI-20260001"],
        "policy_id":          ["POL-20260001"],
        "claim_id":           [None],
        "reinsurer_id":       ["RNS-MUN001"],
        "reinsurer_name":     ["Munich Re"],
        "treaty_id":          ["TRT-2026-001"],
        "treaty_type":        ["QUOTA_SHARE"],
        "cession_date":       ["2026-01-01"],
        "cession_percentage": [30.0],
        "ceded_premium":      [450.0],
        "ceded_liability":    [450000.0],
        "recovered_amount":   [None],
        "status":             ["ACTIVE"],
        "currency":           ["EUR"],
    })


@pytest.fixture
def sample_data_dir(tmp_path) -> Path:
    """
    UPDATED: creates files matching BOTH SFTP directories.
    /inbound/ entities:  policies + claims  → CSV + SHA256 sidecar
    /reports/ entities:  premiums + reinsurance → plain CSV (no sidecar)

    Tests use this fixture to get ready-made CSVs for validation.
    """
    # inbound entities — also write .sha256 sidecars
    for fname, df in [
        (f"policies_{DATE_STR}.csv",    _make_policies_df()),
        (f"claims_{DATE_STR}.csv",      _make_claims_df()),
    ]:
        p = tmp_path / fname
        df.to_csv(p, index=False)
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        (tmp_path / f"{fname}.sha256").write_text(digest)

    # reports entities — plain CSV, no checksums
    for fname, df in [
        (f"premiums_{DATE_STR}.csv",    _make_premiums_df()),
        (f"reinsurance_{DATE_STR}.csv", _make_reinsurance_df()),
    ]:
        p = tmp_path / fname
        df.to_csv(p, index=False)

    return tmp_path


@pytest.fixture
def inbound_csv_dir(tmp_path) -> Path:
    """Only inbound entity CSVs with SHA256 sidecars."""
    for fname, df in [
        (f"policies_{DATE_STR}.csv", _make_policies_df()),
        (f"claims_{DATE_STR}.csv",   _make_claims_df()),
    ]:
        p = tmp_path / fname
        df.to_csv(p, index=False)
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        (tmp_path / f"{fname}.sha256").write_text(digest)
    return tmp_path


@pytest.fixture
def reports_csv_dir(tmp_path) -> Path:
    """Only reports entity CSVs — no checksums."""
    for fname, df in [
        (f"premiums_{DATE_STR}.csv",    _make_premiums_df()),
        (f"reinsurance_{DATE_STR}.csv", _make_reinsurance_df()),
    ]:
        (tmp_path / fname).to_csv if False else df.to_csv(tmp_path / fname, index=False)
    return tmp_path


# State mock fixtures (NEW)

@pytest.fixture
def mock_sftp_state_empty():
    """SFTPState mock that returns empty state — simulates first run."""
    m = MagicMock()
    m.is_first_run    = True
    m.processed_set   = set()
    m.get_new_files.side_effect = lambda files: files  # all files are new
    m.mark_batch.return_value   = None
    m.save.return_value         = None
    return m


@pytest.fixture
def mock_sftp_state_with_history():
    """SFTPState mock with some files already processed — simulates incremental run."""
    already_seen = {
        f"policies_{DATE_STR}.csv.gpg",
        f"policies_{DATE_STR}.csv.sha256",
    }
    m = MagicMock()
    m.is_first_run   = False
    m.processed_set  = already_seen
    m.get_new_files.side_effect = lambda files: [f for f in files if f not in already_seen]
    m.mark_batch.return_value   = None
    m.save.return_value         = None
    return m


@pytest.fixture
def mock_silver_state_empty():
    """SilverState mock — no dates processed yet."""
    m = MagicMock()
    m.is_first_run   = True
    m.processed_dates = set()
    m.is_processed.return_value = False
    m.get_unprocessed_dates.side_effect = lambda dates, **kw: dates
    m.mark_processed.return_value = None
    m.save.return_value           = None
    return m


@pytest.fixture
def mock_silver_state_with_date():
    """SilverState mock — today's date already processed."""
    m = MagicMock()
    m.is_first_run    = False
    m.processed_dates = {BUSINESS_DATE.isoformat()}
    m.is_processed.return_value = True
    m.get_unprocessed_dates.return_value = []
    m.save.return_value = None
    return m


@pytest.fixture
def mock_gold_state_empty():
    """GoldState mock — no partitions loaded yet."""
    m = MagicMock()
    m.is_first_run  = True
    m.loaded_dates  = set()
    m.needs_load.return_value  = True
    m.mark_loaded.return_value = None
    m.save.return_value        = None
    return m


# GCS client mock (UNCHANGED logic, path updated)

@pytest.fixture
def mock_gcs_client():
    mock_blob   = MagicMock()
    mock_bucket = MagicMock()
    mock_client = MagicMock()
    mock_bucket.blob.return_value             = mock_blob
    mock_client.bucket.return_value           = mock_bucket
    mock_blob.upload_from_filename.return_value = None
    mock_blob.upload_from_string.return_value   = None
    with patch("google.cloud.storage.Client", return_value=mock_client):
        yield mock_client, mock_bucket, mock_blob