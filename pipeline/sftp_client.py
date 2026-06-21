from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import paramiko

from pipeline.config import settings

logger = logging.getLogger(__name__)


class SFTPClient:
    """
    SFTP client that opens one SSH connection and visits multiple directories.

    Example — two directories, one connection:
        with SFTPClient(download_dir=Path("/tmp/dl")) as sftp:
            inbound_files = sftp.list_remote_files("/inbound/")
            sftp.download_files("/inbound/", ["policies_20260514.csv.gpg"])

            report_files = sftp.list_remote_files("/reports/")
            sftp.download_files("/reports/", ["premiums_20260514.csv"])
    """

    def __init__(self, download_dir: Path):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    # Context manager

    def __enter__(self) -> "SFTPClient":
        self._connect()
        return self

    def __exit__(self, *_):
        self._disconnect()

    # Connection

    def _connect(self) -> None:
        logger.info("Connecting SFTP → %s:%s as %s",
                    settings.SFTP_HOST, settings.SFTP_PORT, settings.SFTP_USER)
        pkey = paramiko.RSAKey.from_private_key_file(str(settings.SFTP_SSH_KEY_PATH))
        self._transport = paramiko.Transport((settings.SFTP_HOST, settings.SFTP_PORT))
        self._transport.connect(username=settings.SFTP_USER, pkey=pkey)

        # Host-key verification — never disable
        host_keys  = paramiko.HostKeys(str(settings.SFTP_KNOWN_HOSTS))
        server_key = self._transport.get_remote_server_key()
        if not host_keys.check(settings.SFTP_HOST, server_key):
            self._transport.close()
            raise ValueError(
                f"Host key mismatch for {settings.SFTP_HOST}. "
                "Update known_hosts or contact the partner."
            )
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        logger.info("SFTP connected.")

    def _disconnect(self) -> None:
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        logger.info("SFTP disconnected.")

    # Directory listing

    def list_remote_files(self, remote_dir: str) -> list[str]:
        """
        List all non-hidden filenames in remote_dir.
        Called once per SFTPDirectory entry in sftp_directories.yaml.
        """
        attrs = self._sftp.listdir_attr(remote_dir)
        files = [a.filename for a in attrs if not a.filename.startswith(".")]
        logger.info("Listed %s → %d files: %s", remote_dir, len(files), files)
        return files

    # Download

    def download_file(self, remote_dir: str, filename: str) -> Path:
        """
        Download one file from remote_dir.
        Stored under download_dir/{dir_slug}/ to avoid name collisions
        between /inbound/policies.csv and /reports/policies.csv.
        """
        remote_path = f"{remote_dir.rstrip('/')}/{filename}"
        dir_slug  = remote_dir.strip("/").replace("/", "_") or "root"
        local_dir = self.download_dir / dir_slug
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / filename

        logger.info("Downloading %s → %s", remote_path, local_path)
        self._sftp.get(remote_path, str(local_path))
        logger.info("Downloaded %s (%d bytes)", filename, local_path.stat().st_size)
        return local_path

    def download_files(self, remote_dir: str,
                       filenames: list[str]) -> list[tuple[str, Path]]:
        """
        Download a list of files from remote_dir.
        Returns (filename, local_path) pairs.
        Raises immediately on first failure — no silent partial downloads.
        """
        results = []
        for fn in filenames:
            try:
                lp = self.download_file(remote_dir, fn)
                results.append((fn, lp))
            except Exception as exc:
                logger.error("Failed to download %s from %s: %s", fn, remote_dir, exc)
                raise
        return results

    # Archive

    def archive_remote_file(self, remote_dir: str, filename: str,
                            archive_subdir: str = "processed/") -> None:
        """
        Move a processed file to an archive folder on the SFTP server.
        Provides a second layer of protection against re-ingestion
        (state.py is the primary guard).
        Never raises — logs a warning if rename fails.
        """
        src = f"{remote_dir.rstrip('/')}/{filename}"
        dst = f"{remote_dir.rstrip('/')}/{archive_subdir}{filename}"
        try:
            self._sftp.rename(src, dst)
            logger.info("Archived %s → %s", src, dst)
        except Exception as exc:
            logger.warning("Could not archive %s: %s (state guard still active)", src, exc)