#   1. All calls now use validate_directory() instead of validate_batch()
#      because validate_directory() is the new unified entry point that
#      accepts the use_checksum flag from SFTPDirectory config.
#   2. New test class TestUseChecksumFlag:
#        use_checksum=True  (/inbound/) → validation runs
#        use_checksum=False (/reports/) → validation skipped entirely
#   3. Original compute_sha256 and validate_checksum unit tests kept.

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from pipeline.checksum_validator import (
    compute_sha256,
    validate_checksum,
    validate_directory,
)

# Fixtures

@pytest.fixture
def csv_with_sidecar(tmp_path):
    """A CSV file with a correct .sha256 sidecar — simulates /inbound/ files."""
    data   = b"policy_id,premium\nPOL-20260001,1500.00\n"
    f_data = tmp_path / "policies_20260514.csv"
    f_data.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    f_side = tmp_path / "policies_20260514.csv.sha256"
    f_side.write_text(digest)
    return f_data, f_side, digest


@pytest.fixture
def csv_without_sidecar(tmp_path):
    """A plain CSV with no sidecar — simulates /reports/ files."""
    p = tmp_path / "premiums_20260514.csv"
    p.write_bytes(b"premium_id,amount\nPRM-20260001,125.00\n")
    return p

# TestComputeSha256 — unchanged logic

class TestComputeSha256:

    def test_matches_known_digest(self, csv_with_sidecar):
        f_data, _, digest = csv_with_sidecar
        assert compute_sha256(f_data) == digest

    def test_different_content_different_hash(self, tmp_path):
        a = tmp_path / "a.csv"; a.write_bytes(b"content_a")
        b = tmp_path / "b.csv"; b.write_bytes(b"content_b")
        assert compute_sha256(a) != compute_sha256(b)

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.csv"; f.write_bytes(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert compute_sha256(f) == expected


# TestValidateChecksum — unchanged logic

class TestValidateChecksum:

    def test_passes_with_correct_sidecar(self, csv_with_sidecar):
        f_data, f_side, _ = csv_with_sidecar
        assert validate_checksum(f_data, f_side) is True

    def test_raises_on_mismatch(self, csv_with_sidecar, tmp_path):
        f_data, _, _ = csv_with_sidecar
        bad_sidecar  = tmp_path / "bad.sha256"
        bad_sidecar.write_text("0" * 64)   # wrong digest
        with pytest.raises(ValueError, match="MISMATCH"):
            validate_checksum(f_data, bad_sidecar)

    def test_raises_on_tampered_file(self, csv_with_sidecar):
        f_data, f_side, _ = csv_with_sidecar
        # Tamper with the data file after sidecar was written
        f_data.write_bytes(b"TAMPERED CONTENT")
        with pytest.raises(ValueError, match="MISMATCH"):
            validate_checksum(f_data, f_side)

    def test_sidecar_with_filename_prefix(self, tmp_path):
        """Some tools write 'HASH  filename' format — must handle both."""
        data   = b"col1,col2\nval1,val2\n"
        f_data = tmp_path / "data.csv"
        f_data.write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()

        # Write sidecar in "hash  filename" format
        f_side = tmp_path / "data.csv.sha256"
        f_side.write_text(f"{digest}  data.csv\n")

        assert validate_checksum(f_data, f_side) is True

# TestUseChecksumFlag — NEW: covers the multi-dir use_checksum flag

class TestUseChecksumFlag:

    def test_inbound_with_checksum_true_validates(self, tmp_path):
        """
        /inbound/ has use_checksum=True → validate_directory() must
        verify all .sha256 sidecars and return results dict.
        """
        data   = b"policy_id,premium\nPOL-20260001,1500.00\n"
        f_data = tmp_path / "policies_20260514.csv"
        f_data.write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()
        (tmp_path / "policies_20260514.csv.sha256").write_text(digest)

        results = validate_directory(
            data_dir=tmp_path,
            use_checksum=True,
            checksum_ext=".sha256",
        )

        assert "policies_20260514.csv" in results
        assert results["policies_20260514.csv"] is True

    def test_reports_with_checksum_false_skips(self, tmp_path, csv_without_sidecar):
        """
        /reports/ has use_checksum=False → validate_directory() must
        return empty dict immediately without scanning for sidecars.
        """
        results = validate_directory(
            data_dir=tmp_path,
            use_checksum=False,
            checksum_ext=".sha256",
        )
        assert results == {}

    def test_checksum_false_does_not_raise_even_if_files_have_no_sidecar(
        self, tmp_path, csv_without_sidecar
    ):
        """
        Even if the directory contains CSV files with no sidecars,
        use_checksum=False must never raise.
        """
        # premiums_20260514.csv exists but no .sha256
        result = validate_directory(
            data_dir=tmp_path,
            use_checksum=False,
        )
        assert result == {}

    def test_raises_on_checksum_mismatch_when_use_checksum_true(self, tmp_path):
        """
        When use_checksum=True, a mismatched sidecar must raise RuntimeError.
        This is the guard against corrupted or tampered files.
        """
        data   = b"policy_id,premium\nPOL-20260001,1500.00\n"
        f_data = tmp_path / "policies_20260514.csv"
        f_data.write_bytes(data)
        # Write WRONG digest
        (tmp_path / "policies_20260514.csv.sha256").write_text("0" * 64)

        with pytest.raises(RuntimeError, match="Checksum validation FAILED"):
            validate_directory(
                data_dir=tmp_path,
                use_checksum=True,
            )

    def test_multiple_files_all_pass(self, tmp_path):
        """All sidecars correct → all pass → no exception raised."""
        for name, content in [
            ("policies_20260514.csv",  b"pol_id\nPOL-00000001\n"),
            ("claims_20260514.csv",    b"clm_id\nCLM-00000001\n"),
        ]:
            p = tmp_path / name
            p.write_bytes(content)
            digest = hashlib.sha256(content).hexdigest()
            (tmp_path / f"{name}.sha256").write_text(digest)

        results = validate_directory(tmp_path, use_checksum=True)
        assert len(results) == 2
        assert all(results.values())

    def test_no_sidecars_in_dir_logs_warning_and_returns_empty(self, tmp_path):
        """
        If use_checksum=True but no .sha256 files found,
        return empty dict with a warning (not an error).
        Handles edge case where partner forgot to upload sidecars.
        """
        (tmp_path / "policies_20260514.csv").write_bytes(b"col\nval\n")
        # No .sha256 file written

        results = validate_directory(tmp_path, use_checksum=True)
        assert results == {}