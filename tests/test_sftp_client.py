from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import paramiko
import pytest

from pipeline.sftp_client import SFTPClient


# Fixtures

@pytest.fixture
def mock_transport():
    t = MagicMock(spec=paramiko.Transport)
    t.get_remote_server_key.return_value = MagicMock()
    return t


@pytest.fixture
def mock_sftp_channel():
    return MagicMock(spec=paramiko.SFTPClient)


@pytest.fixture
def mock_host_keys():
    hk = MagicMock(spec=paramiko.HostKeys)
    hk.check.return_value = True
    return hk


@pytest.fixture
def connected_client(tmp_path, mock_transport, mock_sftp_channel, mock_host_keys):
    """Return a pre-connected SFTPClient with all paramiko calls mocked."""
    with patch("paramiko.Transport",                 return_value=mock_transport), \
         patch("paramiko.RSAKey.from_private_key_file", return_value=MagicMock()), \
         patch("paramiko.HostKeys",                  return_value=mock_host_keys), \
         patch("paramiko.SFTPClient.from_transport", return_value=mock_sftp_channel):
        client = SFTPClient(download_dir=tmp_path)
        client._transport = mock_transport
        client._sftp      = mock_sftp_channel
        yield client, mock_sftp_channel, tmp_path


# list_remote_files

class TestListRemoteFiles:

    def test_returns_filenames_for_inbound(self, connected_client):
        client, sftp_ch, _ = connected_client
        a1 = MagicMock(); a1.filename = "policies_20260514.csv.gpg"
        a2 = MagicMock(); a2.filename = "claims_20260514.csv.gpg"
        sftp_ch.listdir_attr.return_value = [a1, a2]

        result = client.list_remote_files("/inbound/")
        assert result == ["policies_20260514.csv.gpg", "claims_20260514.csv.gpg"]
        sftp_ch.listdir_attr.assert_called_once_with("/inbound/")

    def test_returns_filenames_for_reports(self, connected_client):
        client, sftp_ch, _ = connected_client
        a1 = MagicMock(); a1.filename = "premiums_20260514.csv"
        a2 = MagicMock(); a2.filename = "reinsurance_20260514.csv"
        sftp_ch.listdir_attr.return_value = [a1, a2]

        result = client.list_remote_files("/reports/")
        assert result == ["premiums_20260514.csv", "reinsurance_20260514.csv"]
        sftp_ch.listdir_attr.assert_called_once_with("/reports/")

    def test_lists_both_directories_in_one_session(self, connected_client):
        """Critical: one connection must serve both /inbound/ and /reports/."""
        client, sftp_ch, _ = connected_client

        def listdir_side_effect(remote_dir):
            if remote_dir == "/inbound/":
                a = MagicMock(); a.filename = "policies_20260514.csv.gpg"
                return [a]
            if remote_dir == "/reports/":
                a = MagicMock(); a.filename = "premiums_20260514.csv"
                return [a]
            return []

        sftp_ch.listdir_attr.side_effect = listdir_side_effect

        inbound_files = client.list_remote_files("/inbound/")
        reports_files = client.list_remote_files("/reports/")

        assert inbound_files == ["policies_20260514.csv.gpg"]
        assert reports_files == ["premiums_20260514.csv"]
        # Only two listdir calls — one per directory
        assert sftp_ch.listdir_attr.call_count == 2

    def test_skips_dotfiles(self, connected_client):
        client, sftp_ch, _ = connected_client
        dot = MagicMock(); dot.filename = ".keep"
        real = MagicMock(); real.filename = "policies_20260514.csv.gpg"
        sftp_ch.listdir_attr.return_value = [dot, real]

        result = client.list_remote_files("/inbound/")
        assert ".keep" not in result
        assert "policies_20260514.csv.gpg" in result


# download_file

class TestDownloadFile:

    def test_inbound_file_stored_under_inbound_subdir(self, connected_client):
        """
        Files from /inbound/ must land in download_dir/inbound/filename
        to avoid collision with /reports/ files of the same name.
        """
        client, sftp_ch, tmp_path = connected_client

        def fake_get(remote, local):
            Path(local).write_bytes(b"encrypted content")

        sftp_ch.get.side_effect = fake_get

        result = client.download_file("/inbound/", "policies_20260514.csv.gpg")

        assert result.parent.name == "inbound"
        assert result.name == "policies_20260514.csv.gpg"
        assert result.exists()

    def test_reports_file_stored_under_reports_subdir(self, connected_client):
        client, sftp_ch, tmp_path = connected_client

        def fake_get(remote, local):
            Path(local).write_bytes(b"plain csv content")

        sftp_ch.get.side_effect = fake_get

        result = client.download_file("/reports/", "premiums_20260514.csv")

        assert result.parent.name == "reports"
        assert result.name == "premiums_20260514.csv"

    def test_no_filename_collision_between_directories(self, connected_client):
        """
        If both /inbound/ and /reports/ had a file called 'data.csv'
        they must land in different subdirs and not overwrite each other.
        """
        client, sftp_ch, tmp_path = connected_client

        def fake_get(remote, local):
            Path(local).write_bytes(remote.encode())

        sftp_ch.get.side_effect = fake_get

        path_a = client.download_file("/inbound/", "data.csv")
        path_b = client.download_file("/reports/", "data.csv")

        assert path_a != path_b
        assert path_a.read_bytes() != path_b.read_bytes()

    def test_remote_path_constructed_correctly_for_inbound(self, connected_client):
        client, sftp_ch, _ = connected_client
        sftp_ch.get.side_effect = lambda r, l: Path(l).write_bytes(b"x")

        client.download_file("/inbound/", "policies_20260514.csv.gpg")

        call_args = sftp_ch.get.call_args[0]
        assert call_args[0] == "/inbound/policies_20260514.csv.gpg"

    def test_remote_path_constructed_correctly_for_reports(self, connected_client):
        client, sftp_ch, _ = connected_client
        sftp_ch.get.side_effect = lambda r, l: Path(l).write_bytes(b"x")

        client.download_file("/reports/", "premiums_20260514.csv")

        call_args = sftp_ch.get.call_args[0]
        assert call_args[0] == "/reports/premiums_20260514.csv"

    def test_raises_on_download_failure(self, connected_client):
        client, sftp_ch, _ = connected_client
        sftp_ch.get.side_effect = IOError("Network timeout")

        with pytest.raises(IOError, match="Network timeout"):
            client.download_file("/inbound/", "missing.csv.gpg")


# download_files (batch)

class TestDownloadFiles:

    def test_batch_download_inbound(self, connected_client):
        client, sftp_ch, _ = connected_client
        sftp_ch.get.side_effect = lambda r, l: Path(l).write_bytes(b"data")

        pairs = client.download_files(
            "/inbound/",
            ["policies_20260514.csv.gpg", "claims_20260514.csv.gpg"],
        )

        assert len(pairs) == 2
        assert all(fn.endswith(".gpg") for fn, _ in pairs)
        assert all(p.parent.name == "inbound" for _, p in pairs)

    def test_batch_download_reports(self, connected_client):
        client, sftp_ch, _ = connected_client
        sftp_ch.get.side_effect = lambda r, l: Path(l).write_bytes(b"csv")

        pairs = client.download_files(
            "/reports/",
            ["premiums_20260514.csv", "reinsurance_20260514.csv"],
        )

        assert len(pairs) == 2
        assert all(p.parent.name == "reports" for _, p in pairs)

    def test_batch_raises_on_first_failure(self, connected_client):
        client, sftp_ch, _ = connected_client
        sftp_ch.get.side_effect = IOError("SFTP error")

        with pytest.raises(IOError):
            client.download_files("/inbound/", ["fail.csv.gpg"])


# archive_remote_file

class TestArchiveRemoteFile:

    def test_archives_inbound_file(self, connected_client):
        client, sftp_ch, _ = connected_client
        client.archive_remote_file("/inbound/", "policies_20260514.csv.gpg")
        sftp_ch.rename.assert_called_once_with(
            "/inbound/policies_20260514.csv.gpg",
            "/inbound/processed/policies_20260514.csv.gpg",
        )

    def test_archives_reports_file(self, connected_client):
        client, sftp_ch, _ = connected_client
        client.archive_remote_file("/reports/", "premiums_20260514.csv")
        sftp_ch.rename.assert_called_once_with(
            "/reports/premiums_20260514.csv",
            "/reports/processed/premiums_20260514.csv",
        )

    def test_archive_failure_does_not_raise(self, connected_client):
        """Archive failure must only log a warning — never crash the pipeline."""
        client, sftp_ch, _ = connected_client
        sftp_ch.rename.side_effect = IOError("rename failed")

        # Must not raise
        client.archive_remote_file("/inbound/", "policies_20260514.csv.gpg")

    def test_custom_archive_subdir(self, connected_client):
        client, sftp_ch, _ = connected_client
        client.archive_remote_file("/inbound/", "policies_20260514.csv.gpg", "done/")
        sftp_ch.rename.assert_called_once_with(
            "/inbound/policies_20260514.csv.gpg",
            "/inbound/done/policies_20260514.csv.gpg",
        )


# Context manager + host key

class TestContextManager:

    def test_disconnects_on_exit(self, tmp_path, mock_transport,
                                  mock_sftp_channel, mock_host_keys):
        with patch("paramiko.Transport",                 return_value=mock_transport), \
             patch("paramiko.RSAKey.from_private_key_file", return_value=MagicMock()), \
             patch("paramiko.HostKeys",                  return_value=mock_host_keys), \
             patch("paramiko.SFTPClient.from_transport", return_value=mock_sftp_channel):
            with SFTPClient(download_dir=tmp_path):
                pass

        mock_sftp_channel.close.assert_called()
        mock_transport.close.assert_called()

    def test_host_key_mismatch_raises(self, tmp_path, mock_transport, mock_host_keys):
        mock_host_keys.check.return_value = False  # mismatch

        with patch("paramiko.Transport",                 return_value=mock_transport), \
             patch("paramiko.RSAKey.from_private_key_file", return_value=MagicMock()), \
             patch("paramiko.HostKeys",                  return_value=mock_host_keys), \
             patch("paramiko.SFTPClient.from_transport", return_value=MagicMock()):
            with pytest.raises(ValueError, match="Host key mismatch"):
                with SFTPClient(download_dir=tmp_path):
                    pass