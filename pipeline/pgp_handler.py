from __future__ import annotations

import gnupg
import logging
import shutil
from pathlib import Path
from typing import Optional

from pipeline.config import settings, get_dir_pgp_passphrase, get_dir_pgp_key_path

logger = logging.getLogger(__name__)


class PGPHandler:

    def __init__(self, dir_id: Optional[str] = None):
        """
        dir_id: SFTP directory ID from sftp_directories.yaml (e.g. "inbound")
                When None falls back to global PGP settings (backward compat).
        """
        self.dir_id = dir_id

        # Isolated GPG home per directory prevents parallel runs from
        # interfering with each other's keyrings.
        # /tmp/gnupg/inbound/   for inbound
        # /tmp/gnupg/reports/   for reports (no keys imported here)
        # /tmp/gnupg/           fallback when no dir_id
        if dir_id:
            gpg_home = settings.PGP_GNUPGHOME / dir_id
        else:
            gpg_home = settings.PGP_GNUPGHOME

        gpg_home.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._gpg_home = gpg_home

        self.gpg = gnupg.GPG(gnupghome=str(gpg_home))
        self.gpg.encoding = "utf-8"

        # Load passphrase for this directory
        self._passphrase = (
            get_dir_pgp_passphrase(dir_id) if dir_id
            else settings.PGP_PASSPHRASE.get_secret_value()
        )

        # Import keys only if this directory uses PGP encryption.
        # Calling _import_keys() for a non-PGP directory like /reports/
        # would fail because no private key exists for it.
        # We defer key import to process_directory() which knows use_pgp.
        self._keys_imported = False

    # Key management

    def _ensure_keys_imported(self) -> None:
        """Import public + private keys lazily on first decrypt call."""
        if self._keys_imported:
            return
        self._import_key(settings.PGP_PUBLIC_KEY_PATH, "public")
        private_key_path = (
            get_dir_pgp_key_path(self.dir_id) if self.dir_id
            else settings.PGP_PRIVATE_KEY_PATH
        )
        self._import_key(private_key_path, f"private ({self.dir_id or 'global'})")
        self._keys_imported = True

    def _import_key(self, path: Path, label: str) -> None:
        if not path.exists():
            raise FileNotFoundError(f"PGP {label} key not found: {path}")
        result = self.gpg.import_keys(path.read_text())
        if result.count == 0:
            raise RuntimeError(f"Failed to import {label} key from {path}")
        logger.info("Imported %d PGP key(s) [%s]", result.count, label)

    # Main entry point used by the DAG decrypt task

    def process_directory(
        self,
        use_pgp: bool,
        download_dir: Path,
        decrypt_dir: Path,
        downloaded_files: list[str],
    ) -> list[str]:
        """
        Process all downloaded files for one SFTP directory.

        use_pgp=True  (/inbound/):
            - Import keys (lazy, first call only)
            - Find all .gpg files under download_dir
            - Decrypt each to decrypt_dir (strips .gpg extension)
            - Copy .sha256 sidecar files to decrypt_dir unchanged
            - Returns list of plain CSV filenames now in decrypt_dir

        use_pgp=False (/reports/):
            - Find all .csv files under download_dir
            - Copy straight to decrypt_dir (no decryption)
            - Returns list of CSV filenames now in decrypt_dir

        Both cases produce the same output structure in decrypt_dir:
            plain .csv files ready for checksum validation and schema validation.
        """
        decrypt_dir.mkdir(parents=True, exist_ok=True)

        # Collect all local files from the namespaced subdirectory.
        # sftp_client.download_file() stores files under:
        #   download_dir/{dir_slug}/{filename}
        # e.g. /tmp/dl_inbound/inbound/policies_20260514.csv.gpg
        local_files: list[Path] = []
        for item in download_dir.rglob("*"):
            if item.is_file():
                local_files.append(item)

        if not local_files:
            logger.warning(
                "[%s] No local files found under %s",
                self.dir_id or "global", download_dir,
            )
            return []

        result_files: list[str] = []

        if use_pgp:
            # Decrypt .gpg files
            self._ensure_keys_imported()

            gpg_files = [f for f in local_files if f.suffix == ".gpg"]
            sha_files = [f for f in local_files if f.name.endswith(".sha256")]

            for gpg_file in gpg_files:
                out_name = gpg_file.stem   # strips .gpg: policies_20260514.csv.gpg → .csv
                out_path = decrypt_dir / out_name
                self.decrypt_file(gpg_file, out_path)
                result_files.append(out_name)

            # Copy checksum sidecars unchanged — needed by checksum_validator
            for sha_file in sha_files:
                dst = decrypt_dir / sha_file.name
                shutil.copy(sha_file, dst)
                logger.debug("Copied sidecar %s → %s", sha_file.name, dst)

            logger.info(
                "[%s] Decrypted %d files, copied %d sidecars → %s",
                self.dir_id or "global", len(gpg_files), len(sha_files), decrypt_dir,
            )

        else:
            # Plain CSV: copy without decryption
            csv_files = [f for f in local_files if f.suffix == ".csv"]
            for csv_file in csv_files:
                dst = decrypt_dir / csv_file.name
                shutil.copy(csv_file, dst)
                result_files.append(csv_file.name)

            logger.info(
                "[%s] use_pgp=False — copied %d CSV files → %s",
                self.dir_id or "global", len(csv_files), decrypt_dir,
            )

        return result_files

    # Low-level decrypt (called by process_directory and tests)

    def decrypt_file(self, encrypted_path: Path, output_path: Path) -> Path:
        """Decrypt one .gpg file using the directory's private key."""
        with open(encrypted_path, "rb") as fh:
            result = self.gpg.decrypt_file(
                fh,
                passphrase=self._passphrase,
                output=str(output_path),
                always_trust=True,
            )
        if not result.ok:
            raise RuntimeError(
                f"Decryption failed [{self.dir_id}] {encrypted_path.name}: "
                f"status={result.status} stderr={result.stderr}"
            )
        logger.info(
            "Decrypted %s → %s (%d bytes)",
            encrypted_path.name, output_path.name, output_path.stat().st_size,
        )
        return output_path

    def decrypt_batch(self, encrypted_paths: list[Path], out_dir: Path) -> list[Path]:
        """Decrypt multiple .gpg files into out_dir."""
        out_dir.mkdir(parents=True, exist_ok=True)
        results = []
        for enc in encrypted_paths:
            out_name = enc.stem if enc.suffix == ".gpg" else enc.name
            results.append(self.decrypt_file(enc, out_dir / out_name))
        return results

    # Encryption (used by tests to create .gpg fixtures)

    def encrypt_file(
        self,
        plaintext_path: Path,
        output_path: Path,
        recipient_fingerprint: str,
    ) -> Path:
        """
        Encrypt a plaintext file for a recipient using their public key.
        Used by tests to produce .gpg fixtures — simulates what the partner does.
        """
        self._ensure_keys_imported()
        result = self.gpg.encrypt(
            plaintext_path.read_bytes(),
            recipients=[recipient_fingerprint],
            always_trust=True,
            armor=False,
            output=str(output_path),
        )
        if not result.ok:
            raise RuntimeError(
                f"Encryption failed: {result.status} / {result.stderr}"
            )
        logger.info("Encrypted %s → %s", plaintext_path.name, output_path.name)
        return output_path

    # Cleanup

    def cleanup_gpg_home(self) -> None:
        """
        Remove this directory's isolated GPG home.
        UPDATED: removes only /tmp/gnupg/{dir_id}/ not the whole /tmp/gnupg/
        so parallel TaskGroups don't destroy each other's keyrings.
        """
        shutil.rmtree(str(self._gpg_home), ignore_errors=True)
        logger.info("Cleaned GPG home: %s", self._gpg_home)