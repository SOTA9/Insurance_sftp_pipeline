#   1. All tests now instantiate PGPHandler(dir_id="inbound") or
#      PGPHandler(dir_id="reports") to mirror real usage.
#   2. New test class TestProcessDirectory — covers the main DAG entry point:
#        process_directory(use_pgp=True)  → /inbound/ path
#        process_directory(use_pgp=False) → /reports/ path
#   3. TestIsolatedGpgHome — new class verifying each dir_id gets its own
#      GPG home so parallel TaskGroups don't interfere.
#   4. TestCleanup updated — cleanup only removes {dir_id} subdir not all
#      of /tmp/gnupg/ so parallel directories are not affected.
#   5. Original encrypt/decrypt round-trip tests kept and extended.

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipeline.pgp_handler import PGPHandler

PLAINTEXT_POLICIES     = b"policy_id,premium\nPOL-20260001,1500.00\nPOL-20260002,2300.00\n"
PLAINTEXT_PREMIUMS     = b"premium_id,amount_due\nPRM-20260001,125.00\nPRM-20260002,191.67\n"


# Fixtures

@pytest.fixture
def handler_inbound(mock_settings, pgp_keypair):
    """PGPHandler for /inbound/ (use_pgp=True directory)."""
    return PGPHandler(dir_id="inbound")


@pytest.fixture
def handler_reports(mock_settings):
    """PGPHandler for /reports/ (use_pgp=False directory — no PGP keys needed)."""
    return PGPHandler(dir_id="reports")


@pytest.fixture
def plaintext_policies(tmp_path) -> Path:
    p = tmp_path / "policies_20260514.csv"
    p.write_bytes(PLAINTEXT_POLICIES)
    return p


@pytest.fixture
def plaintext_premiums(tmp_path) -> Path:
    p = tmp_path / "premiums_20260514.csv"
    p.write_bytes(PLAINTEXT_PREMIUMS)
    return p


@pytest.fixture
def encrypted_policies(tmp_path, plaintext_policies, handler_inbound, pgp_keypair) -> Path:
    out = tmp_path / "policies_20260514.csv.gpg"
    handler_inbound._ensure_keys_imported()
    handler_inbound.encrypt_file(plaintext_policies, out, pgp_keypair["fingerprint"])
    return out

# TestKeyImport

class TestKeyImport:

    def test_inbound_handler_initialises(self, handler_inbound):
        """PGPHandler for inbound dir initialises without error."""
        assert handler_inbound is not None
        assert handler_inbound.dir_id == "inbound"

    def test_reports_handler_initialises_without_keys(self, handler_reports):
        """PGPHandler for reports dir (no PGP) initialises without importing keys."""
        assert handler_reports is not None
        assert handler_reports.dir_id == "reports"
        # Keys not imported because reports dir never calls _ensure_keys_imported
        assert handler_reports._keys_imported is False

    def test_missing_public_key_raises(self, tmp_path, mock_settings):
        mock_settings.PGP_PUBLIC_KEY_PATH = tmp_path / "missing_pub.asc"
        handler = PGPHandler(dir_id="inbound")
        with pytest.raises(FileNotFoundError, match="public"):
            handler._ensure_keys_imported()

    def test_missing_private_key_raises(self, tmp_path, mock_settings, pgp_public_key_file):
        mock_settings.PGP_PUBLIC_KEY_PATH = pgp_public_key_file
        with patch("pipeline.config.get_dir_pgp_key_path",
                   return_value=tmp_path / "missing_priv.asc"):
            handler = PGPHandler(dir_id="inbound")
            with pytest.raises(FileNotFoundError, match="private"):
                handler._ensure_keys_imported()


# TestIsolatedGpgHome

class TestIsolatedGpgHome:

    def test_inbound_uses_inbound_subdir(self, mock_settings, handler_inbound):
        """
        /inbound/ handler GPG home must be /tmp/gnupg/inbound/
        not /tmp/gnupg/ — prevents keyring contamination.
        """
        expected = mock_settings.PGP_GNUPGHOME / "inbound"
        assert handler_inbound._gpg_home == expected

    def test_reports_uses_reports_subdir(self, mock_settings, handler_reports):
        expected = mock_settings.PGP_GNUPGHOME / "reports"
        assert handler_reports._gpg_home == expected

    def test_two_handlers_have_different_gpg_homes(self, handler_inbound, handler_reports):
        assert handler_inbound._gpg_home != handler_reports._gpg_home


# TestProcessDirectory — main DAG entry point

class TestProcessDirectory:

    def test_inbound_decrypts_gpg_files(
        self, tmp_path, handler_inbound, encrypted_policies, pgp_keypair
    ):
        """
        process_directory(use_pgp=True) must decrypt .gpg files and
        produce plain .csv files in decrypt_dir.
        """
        # Set up download_dir with namespaced subdir (as sftp_client does)
        dl_dir      = tmp_path / "dl_inbound"
        inbound_sub = dl_dir / "inbound"
        inbound_sub.mkdir(parents=True)

        # Place encrypted file in namespaced subdir
        (inbound_sub / "policies_20260514.csv.gpg").write_bytes(
            encrypted_policies.read_bytes()
        )

        # Also place a .sha256 sidecar
        (inbound_sub / "policies_20260514.csv.sha256").write_text("abc123")

        decrypt_dir = tmp_path / "decrypted"
        result_files = handler_inbound.process_directory(
            use_pgp=True,
            download_dir=dl_dir,
            decrypt_dir=decrypt_dir,
            downloaded_files=["policies_20260514.csv.gpg",
                              "policies_20260514.csv.sha256"],
        )

        # Decrypted CSV must exist
        assert (decrypt_dir / "policies_20260514.csv").exists()
        # Sidecar must be copied
        assert (decrypt_dir / "policies_20260514.csv.sha256").exists()
        assert "policies_20260514.csv" in result_files

    def test_inbound_decrypted_content_matches_original(
        self, tmp_path, handler_inbound, encrypted_policies
    ):
        dl_dir      = tmp_path / "dl"
        inbound_sub = dl_dir / "inbound"
        inbound_sub.mkdir(parents=True)
        (inbound_sub / "policies_20260514.csv.gpg").write_bytes(
            encrypted_policies.read_bytes()
        )
        decrypt_dir = tmp_path / "dec"
        handler_inbound.process_directory(
            use_pgp=True,
            download_dir=dl_dir,
            decrypt_dir=decrypt_dir,
            downloaded_files=["policies_20260514.csv.gpg"],
        )
        result = (decrypt_dir / "policies_20260514.csv").read_bytes()
        assert result == PLAINTEXT_POLICIES

    def test_reports_copies_csv_without_decryption(
        self, tmp_path, handler_reports, plaintext_premiums
    ):
        """
        process_directory(use_pgp=False) must copy plain CSV files
        without touching GPG at all.
        """
        dl_dir      = tmp_path / "dl_reports"
        reports_sub = dl_dir / "reports"
        reports_sub.mkdir(parents=True)
        (reports_sub / "premiums_20260514.csv").write_bytes(PLAINTEXT_PREMIUMS)

        decrypt_dir  = tmp_path / "dec"
        result_files = handler_reports.process_directory(
            use_pgp=False,
            download_dir=dl_dir,
            decrypt_dir=decrypt_dir,
            downloaded_files=["premiums_20260514.csv"],
        )

        out = decrypt_dir / "premiums_20260514.csv"
        assert out.exists()
        assert out.read_bytes() == PLAINTEXT_PREMIUMS
        assert "premiums_20260514.csv" in result_files

    def test_reports_does_not_touch_gpg(self, tmp_path, handler_reports):
        """
        process_directory(use_pgp=False) must never call gpg.decrypt_file.
        """
        dl_dir      = tmp_path / "dl"
        reports_sub = dl_dir / "reports"
        reports_sub.mkdir(parents=True)
        (reports_sub / "premiums_20260514.csv").write_bytes(PLAINTEXT_PREMIUMS)

        with patch.object(handler_reports, "decrypt_file") as mock_decrypt:
            handler_reports.process_directory(
                use_pgp=False,
                download_dir=dl_dir,
                decrypt_dir=tmp_path / "dec",
                downloaded_files=["premiums_20260514.csv"],
            )
            mock_decrypt.assert_not_called()

    def test_returns_empty_list_when_no_files(self, tmp_path, handler_reports):
        dl_dir = tmp_path / "dl_empty"
        dl_dir.mkdir()
        result = handler_reports.process_directory(
            use_pgp=False,
            download_dir=dl_dir,
            decrypt_dir=tmp_path / "dec",
            downloaded_files=[],
        )
        assert result == []

# TestDecryptFile — low-level decrypt

class TestDecryptFile:

    def test_decrypt_recovers_plaintext(
        self, tmp_path, handler_inbound, encrypted_policies
    ):
        out = tmp_path / "out.csv"
        handler_inbound.decrypt_file(encrypted_policies, out)
        assert out.read_bytes() == PLAINTEXT_POLICIES

    def test_decrypt_wrong_passphrase_raises(
        self, tmp_path, encrypted_policies, mock_settings, pgp_public_key_file
    ):
        mock_settings.PGP_PASSPHRASE.get_secret_value.return_value = "wrong-passphrase"
        with patch("pipeline.config.get_dir_pgp_passphrase",
                   return_value="wrong-passphrase"):
            bad_handler = PGPHandler(dir_id="inbound")
            with pytest.raises(RuntimeError, match="Decryption failed"):
                bad_handler.decrypt_file(encrypted_policies, tmp_path / "out.csv")


# TestDecryptBatch

class TestDecryptBatch:

    def test_batch_decrypts_multiple_files(
        self, tmp_path, handler_inbound, pgp_keypair
    ):
        # Encrypt 3 files
        enc_files = []
        for i in range(3):
            pt = tmp_path / f"file_{i}.csv"
            pt.write_bytes(f"id,value\n{i},test\n".encode())
            enc = tmp_path / f"file_{i}.csv.gpg"
            handler_inbound._ensure_keys_imported()
            handler_inbound.encrypt_file(pt, enc, pgp_keypair["fingerprint"])
            enc_files.append(enc)

        out_dir = tmp_path / "batch_out"
        results = handler_inbound.decrypt_batch(enc_files, out_dir)

        assert len(results) == 3
        assert all(r.exists() for r in results)

    def test_batch_strips_gpg_extension(self, tmp_path, handler_inbound, pgp_keypair):
        pt  = tmp_path / "claims_20260514.csv"
        pt.write_bytes(b"claim_id,amount\nCLM-20260001,5000\n")
        enc = tmp_path / "claims_20260514.csv.gpg"
        handler_inbound._ensure_keys_imported()
        handler_inbound.encrypt_file(pt, enc, pgp_keypair["fingerprint"])

        results = handler_inbound.decrypt_batch([enc], tmp_path / "out")
        assert results[0].name == "claims_20260514.csv"


# TestCleanup

class TestCleanup:

    def test_cleanup_removes_only_dir_specific_gpg_home(
        self, tmp_path, mock_settings
    ):
        """
        cleanup_gpg_home() must remove /tmp/gnupg/inbound/ only,
        not /tmp/gnupg/ — so the /reports/ handler's home is unaffected.
        """
        base     = tmp_path / "gnupg"
        inbound  = base / "inbound"
        reports  = base / "reports"
        inbound.mkdir(parents=True, mode=0o700)
        reports.mkdir(parents=True, mode=0o700)

        mock_settings.PGP_GNUPGHOME = base

        handler = PGPHandler(dir_id="inbound")
        handler.cleanup_gpg_home()

        assert not inbound.exists(), "/tmp/gnupg/inbound/ should be removed"
        assert reports.exists(),     "/tmp/gnupg/reports/ should NOT be removed"

    def test_cleanup_idempotent(self, tmp_path, mock_settings):
        """Calling cleanup twice must not raise."""
        mock_settings.PGP_GNUPGHOME = tmp_path / "gnupg"
        handler = PGPHandler(dir_id="inbound")
        handler.cleanup_gpg_home()
        handler.cleanup_gpg_home()  # second call — must not raise