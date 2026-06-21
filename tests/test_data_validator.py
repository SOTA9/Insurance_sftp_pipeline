#   1. infer_file_type() tests updated — now tested with AND without
#      sftp_dir parameter to cover both code paths:
#        - with sftp_dir → uses SFTPDirectory.files entries (preferred)
#        - without sftp_dir → falls back to prefix matching (original)
#   2. validate_file() tests updated — pass sftp_dir where relevant
#      to reflect how the DAG validate task calls it.
#   3. New TestInferFileTypeWithDirectory class — covers directory-aware
#      entity lookup for both inbound (policies/claims) and reports
#      (premiums/reinsurance).
#   4. New TestDirIdInValidatedData — verifies that when dir_id column
#      is present in a CSV it passes schema validation (nullable column
#      added to all four schemas).
#   5. All original schema rejection tests (bad IDs, invalid enums,
#      negative amounts) kept 100% intact.

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandera as pa
import pytest

from pipeline.data_validator import infer_file_type, validate_file


# TestInferFileType — without sftp_dir (original fallback path)

class TestInferFileTypeNoDir:
    """Original prefix-matching logic — no sftp_dir provided."""

    @pytest.mark.parametrize("filename,expected", [
        ("policies_20260514.csv",    "policies"),
        ("POLICIES_20260514.CSV",    "policies"),
        ("claims_20260514.csv",      "claims"),
        ("premiums_20260514.csv",    "premiums"),
        ("reinsurance_20260514.csv", "reinsurance"),
    ])
    def test_known_prefixes(self, filename, expected):
        assert infer_file_type(filename) == expected

    def test_unknown_prefix_raises(self):
        with pytest.raises(ValueError, match="Cannot determine file type"):
            infer_file_type("invoices_20260514.csv")

    def test_unknown_prefix_no_dir_raises(self):
        with pytest.raises(ValueError):
            infer_file_type("unknown_entity_20260514.csv", sftp_dir=None)


# TestInferFileTypeWithDirectory — directory-aware lookup (new code path)

class TestInferFileTypeWithDirectory:
    """
    New tests: infer_file_type() with sftp_dir uses SFTPDirectory.files
    entries for lookup. Covers both /inbound/ and /reports/ directories.
    """

    def test_inbound_policies(self, sftp_dir_inbound):
        assert infer_file_type("policies_20260514.csv.gpg", sftp_dir_inbound) == "policies"

    def test_inbound_claims(self, sftp_dir_inbound):
        assert infer_file_type("claims_20260514.csv.gpg", sftp_dir_inbound) == "claims"

    def test_reports_premiums(self, sftp_dir_reports):
        assert infer_file_type("premiums_20260514.csv", sftp_dir_reports) == "premiums"

    def test_reports_reinsurance(self, sftp_dir_reports):
        assert infer_file_type("reinsurance_20260514.csv", sftp_dir_reports) == "reinsurance"

    def test_inbound_dir_matches_before_fallback(self, sftp_dir_inbound):
        """When sftp_dir provided, it checks FileEntry.entity first."""
        # policies is in inbound dir so must return "policies" via dir path
        result = infer_file_type("policies_20260514.csv", sftp_dir_inbound)
        assert result == "policies"

    def test_falls_back_to_prefix_when_no_dir_match(self, sftp_dir_inbound):
        """
        If filename does not match any entity in the provided sftp_dir,
        the function falls back to global SCHEMA_MAP prefix matching.
        premiums is NOT in inbound dir but IS in SCHEMA_MAP.
        """
        result = infer_file_type("premiums_20260514.csv", sftp_dir_inbound)
        assert result == "premiums"

    def test_unknown_file_in_dir_raises(self, sftp_dir_inbound):
        """Completely unknown file raises even with sftp_dir."""
        with pytest.raises(ValueError):
            infer_file_type("invoices_20260514.csv", sftp_dir_inbound)


# TestValidateFilePolicies

class TestValidateFilePolicies:

    def test_valid_data_passes(self, sample_data_dir, sftp_dir_inbound):
        csv = sample_data_dir / "policies_20260514.csv"
        df, stats = validate_file(csv, sftp_dir=sftp_dir_inbound)
        assert stats["file_type"] == "policies"
        assert stats["rows_valid"] == 2
        assert stats["rows_dropped"] == 0

    def test_valid_data_passes_without_sftp_dir(self, sample_data_dir):
        """Backward compat: validate_file without sftp_dir still works."""
        csv = sample_data_dir / "policies_20260514.csv"
        df, stats = validate_file(csv)
        assert stats["file_type"] == "policies"

    def test_invalid_policy_id_format_raises(self, tmp_path, sftp_dir_inbound):
        df = pd.DataFrame({
            "policy_id":         ["INVALID-001"],
            "insured_name":      ["Test"],
            "policy_start_date": ["2024-01-01"],
            "policy_end_date":   ["2025-01-01"],
            "premium_amount":    [1000.0],
            "coverage_type":     ["LIFE"],
            "risk_score":        [0.5],
            "broker_id":         [None],
            "status":            ["ACTIVE"],
        })
        csv = tmp_path / "policies_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv, sftp_dir=sftp_dir_inbound)

    def test_negative_premium_raises(self, tmp_path):
        df = pd.DataFrame({
            "policy_id":         ["POL-20260001"],
            "insured_name":      ["Test"],
            "policy_start_date": ["2024-01-01"],
            "policy_end_date":   ["2025-01-01"],
            "premium_amount":    [-500.0],
            "coverage_type":     ["LIFE"],
            "risk_score":        [0.5],
            "broker_id":         [None],
            "status":            ["ACTIVE"],
        })
        csv = tmp_path / "policies_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

    def test_invalid_coverage_type_raises(self, tmp_path):
        df = pd.DataFrame({
            "policy_id":         ["POL-20260001"],
            "insured_name":      ["Test"],
            "policy_start_date": ["2024-01-01"],
            "policy_end_date":   ["2025-01-01"],
            "premium_amount":    [1000.0],
            "coverage_type":     ["EXOTIC"],
            "risk_score":        [0.5],
            "broker_id":         [None],
            "status":            ["ACTIVE"],
        })
        csv = tmp_path / "policies_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

    def test_risk_score_over_one_raises(self, tmp_path):
        df = pd.DataFrame({
            "policy_id":         ["POL-20260001"],
            "insured_name":      ["Test"],
            "policy_start_date": ["2024-01-01"],
            "policy_end_date":   ["2025-01-01"],
            "premium_amount":    [1000.0],
            "coverage_type":     ["HEALTH"],
            "risk_score":        [1.5],
            "broker_id":         [None],
            "status":            ["ACTIVE"],
        })
        csv = tmp_path / "policies_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

# TestValidateFileClaims

class TestValidateFileClaims:

    def test_valid_data_passes(self, sample_data_dir, sftp_dir_inbound):
        csv = sample_data_dir / "claims_20260514.csv"
        df, stats = validate_file(csv, sftp_dir=sftp_dir_inbound)
        assert stats["file_type"] == "claims"
        assert stats["rows_valid"] > 0

    def test_invalid_claim_id_raises(self, tmp_path):
        df = pd.DataFrame({
            "claim_id":           ["C-001"],
            "policy_id":          ["POL-20260001"],
            "claimant_name":      ["Test"],
            "incident_date":      ["2025-03-10"],
            "reported_date":      ["2025-03-15"],
            "claim_type":         ["MEDICAL"],
            "claimed_amount":     [1000.0],
            "approved_amount":    [None],
            "status":             ["OPEN"],
            "adjuster_id":        [None],
            "deductible_applied": [None],
            "fraud_flag":         [False],
        })
        csv = tmp_path / "claims_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

    def test_invalid_status_raises(self, tmp_path):
        df = pd.DataFrame({
            "claim_id":           ["CLM-20260001"],
            "policy_id":          ["POL-20260001"],
            "claimant_name":      ["Test"],
            "incident_date":      ["2025-03-10"],
            "reported_date":      ["2025-03-15"],
            "claim_type":         ["MEDICAL"],
            "claimed_amount":     [1000.0],
            "approved_amount":    [None],
            "status":             ["UNKNOWN_STATUS"],
            "adjuster_id":        [None],
            "deductible_applied": [None],
            "fraud_flag":         [False],
        })
        csv = tmp_path / "claims_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

# TestValidateFilePremiums

class TestValidateFilePremiums:

    def test_valid_data_passes(self, sample_data_dir, sftp_dir_reports):
        csv = sample_data_dir / "premiums_20260514.csv"
        df, stats = validate_file(csv, sftp_dir=sftp_dir_reports)
        assert stats["file_type"] == "premiums"
        assert stats["rows_valid"] > 0

    def test_valid_data_passes_no_dir(self, sample_data_dir):
        csv = sample_data_dir / "premiums_20260514.csv"
        _, stats = validate_file(csv)
        assert stats["file_type"] == "premiums"

    def test_zero_amount_due_raises(self, tmp_path):
        df = pd.DataFrame({
            "premium_id":       ["PRM-20260001"],
            "policy_id":        ["POL-20260001"],
            "payment_date":     [None],
            "due_date":         ["2026-01-01"],
            "amount_due":       [0.0],
            "amount_paid":      [None],
            "payment_method":   [None],
            "currency":         ["EUR"],
            "instalment_number":[1],
            "instalment_total": [12],
            "status":           ["PENDING"],
            "late_fee":         [None],
        })
        csv = tmp_path / "premiums_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

    def test_invalid_currency_raises(self, tmp_path):
        df = pd.DataFrame({
            "premium_id":       ["PRM-20260001"],
            "policy_id":        ["POL-20260001"],
            "payment_date":     [None],
            "due_date":         ["2026-01-01"],
            "amount_due":       [125.0],
            "amount_paid":      [None],
            "payment_method":   [None],
            "currency":         ["EURO"],
            "instalment_number":[1],
            "instalment_total": [12],
            "status":           ["PENDING"],
            "late_fee":         [None],
        })
        csv = tmp_path / "premiums_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)


# TestValidateFileReinsurance

class TestValidateFileReinsurance:

    def test_valid_data_passes(self, sample_data_dir, sftp_dir_reports):
        csv = sample_data_dir / "reinsurance_20260514.csv"
        df, stats = validate_file(csv, sftp_dir=sftp_dir_reports)
        assert stats["file_type"] == "reinsurance"
        assert stats["rows_valid"] > 0

    def test_cession_percentage_over_100_raises(self, tmp_path):
        df = pd.DataFrame({
            "ri_record_id":       ["RI-20260001"],
            "policy_id":          ["POL-20260001"],
            "claim_id":           [None],
            "reinsurer_id":       ["RNS-MUN001"],
            "reinsurer_name":     ["Munich Re"],
            "treaty_id":          ["TRT-2026-001"],
            "treaty_type":        ["QUOTA_SHARE"],
            "cession_date":       ["2026-01-01"],
            "cession_percentage": [110.0],
            "ceded_premium":      [450.0],
            "ceded_liability":    [450000.0],
            "recovered_amount":   [None],
            "status":             ["ACTIVE"],
            "currency":           ["EUR"],
        })
        csv = tmp_path / "reinsurance_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)

    def test_invalid_treaty_type_raises(self, tmp_path):
        df = pd.DataFrame({
            "ri_record_id":       ["RI-20260001"],
            "policy_id":          ["POL-20260001"],
            "claim_id":           [None],
            "reinsurer_id":       ["RNS-MUN001"],
            "reinsurer_name":     ["Munich Re"],
            "treaty_id":          ["TRT-2026-001"],
            "treaty_type":        ["PROPORTIONAL"],
            "cession_date":       ["2026-01-01"],
            "cession_percentage": [30.0],
            "ceded_premium":      [450.0],
            "ceded_liability":    [450000.0],
            "recovered_amount":   [None],
            "status":             ["ACTIVE"],
            "currency":           ["EUR"],
        })
        csv = tmp_path / "reinsurance_20260514.csv"
        df.to_csv(csv, index=False)
        with pytest.raises(pa.errors.SchemaErrors):
            validate_file(csv)


# TestDirIdColumn — NEW: dir_id column added to all schemas

class TestDirIdColumn:
    """
    Verifies that the new dir_id column added to all four schemas
    passes validation when present (nullable, required=False).
    Files without this column also pass (backward compat).
    """

    def test_policies_with_dir_id_passes(self, tmp_path):
        df = pd.DataFrame({
            "policy_id":         ["POL-20260001"],
            "insured_name":      ["Alice"],
            "policy_start_date": ["2024-01-01"],
            "policy_end_date":   ["2025-01-01"],
            "premium_amount":    [1000.0],
            "coverage_type":     ["LIFE"],
            "risk_score":        [0.5],
            "broker_id":         [None],
            "status":            ["ACTIVE"],
            "dir_id":            ["inbound"],   # NEW column
        })
        csv = tmp_path / "policies_20260514.csv"
        df.to_csv(csv, index=False)
        _, stats = validate_file(csv)
        assert stats["rows_valid"] == 1

    def test_claims_with_dir_id_passes(self, tmp_path):
        df = pd.DataFrame({
            "claim_id":           ["CLM-20260001"],
            "policy_id":          ["POL-20260001"],
            "claimant_name":      ["Bob"],
            "incident_date":      ["2025-03-10"],
            "reported_date":      ["2025-03-15"],
            "claim_type":         ["MEDICAL"],
            "claimed_amount":     [5000.0],
            "approved_amount":    [4500.0],
            "status":             ["APPROVED"],
            "adjuster_id":        ["ADJ-001"],
            "deductible_applied": [500.0],
            "fraud_flag":         [False],
            "dir_id":             ["inbound"],   # NEW column
        })
        csv = tmp_path / "claims_20260514.csv"
        df.to_csv(csv, index=False)
        _, stats = validate_file(csv)
        assert stats["rows_valid"] == 1

    def test_premiums_with_dir_id_passes(self, tmp_path):
        df = pd.DataFrame({
            "premium_id":        ["PRM-20260001"],
            "policy_id":         ["POL-20260001"],
            "payment_date":      ["2026-01-01"],
            "due_date":          ["2026-01-01"],
            "amount_due":        [125.0],
            "amount_paid":       [125.0],
            "payment_method":    ["BANK_TRANSFER"],
            "currency":          ["EUR"],
            "instalment_number": [1],
            "instalment_total":  [12],
            "status":            ["PAID"],
            "late_fee":          [0.0],
            "dir_id":            ["reports"],   # NEW column
        })
        csv = tmp_path / "premiums_20260514.csv"
        df.to_csv(csv, index=False)
        _, stats = validate_file(csv)
        assert stats["rows_valid"] == 1

    def test_reinsurance_with_dir_id_passes(self, tmp_path):
        df = pd.DataFrame({
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
            "dir_id":             ["reports"],   # NEW column
        })
        csv = tmp_path / "reinsurance_20260514.csv"
        df.to_csv(csv, index=False)
        _, stats = validate_file(csv)
        assert stats["rows_valid"] == 1

    def test_files_without_dir_id_still_pass(self, sample_data_dir):
        """
        Backward compatibility: files that do NOT have dir_id column
        must still pass validation (required=False in schemas).
        """
        for fname in [
            "policies_20260514.csv",
            "claims_20260514.csv",
            "premiums_20260514.csv",
            "reinsurance_20260514.csv",
        ]:
            csv = sample_data_dir / fname
            if csv.exists():
                _, stats = validate_file(csv)
                assert stats["rows_valid"] > 0, f"{fname} failed without dir_id"

# TestValidateFileStats

class TestValidateFileStats:

    def test_stats_keys_present(self, sample_data_dir):
        _, stats = validate_file(sample_data_dir / "policies_20260514.csv")
        required_keys = {"file", "file_type", "rows_raw", "rows_valid",
                         "rows_dropped", "columns"}
        assert required_keys <= stats.keys()

    def test_rows_dropped_is_non_negative(self, sample_data_dir):
        for fname in ["claims_20260514.csv", "premiums_20260514.csv"]:
            csv = sample_data_dir / fname
            if csv.exists():
                _, stats = validate_file(csv)
                assert stats["rows_dropped"] >= 0

    def test_rows_valid_plus_dropped_equals_raw(self, sample_data_dir):
        _, stats = validate_file(sample_data_dir / "policies_20260514.csv")
        assert stats["rows_valid"] + stats["rows_dropped"] == stats["rows_raw"]