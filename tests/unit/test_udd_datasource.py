"""Tests for UDDDataSource."""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

# Ensure psycopg2 is mocked before import
sys.modules["psycopg2"] = MagicMock()

from debian_observer.ingestion.acquisition import UDDDataSource


class TestUDDDataSource:
    """Unit tests for the UDD PostgreSQL data source."""

    @patch("psycopg2.connect")
    def test_retrieve_queries_packages_table(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("hello", "2.10-3", "debian", "sid", "amd64"),
            ("world", "1.0", "debian", "trixie", "arm64"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="packages", limit=5)
        records = source.retrieve()

        assert len(records) == 2
        assert records[0]["package"] == "hello"
        assert records[0]["version"] == "2.10-3"
        assert records[0]["distribution"] == "debian"
        assert records[0]["release"] == "sid"
        assert records[0]["architecture"] == "amd64"
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "packages" in sql
        assert "LIMIT" in sql

    @patch("psycopg2.connect")
    def test_compute_checksum_deterministic(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("hello", "2.10-3", "debian", "sid", "amd64"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="packages")
        checksum1 = source.compute_checksum()
        checksum2 = source.compute_checksum()
        assert checksum1 == checksum2
        assert len(checksum1) == 64

    @patch("psycopg2.connect")
    def test_uses_env_vars_for_connection(self, mock_connect: MagicMock) -> None:
        """UDDDataSource should read host/user/password from env vars."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("pkg", "1.0", "debian", "sid", "amd64")]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        with patch.dict(
            os.environ,
            {
                "UDD_HOST": "192.168.1.218",
                "UDD_PORT": "5432",
                "UDD_DATABASE": "udd",
                "UDD_USER": "udd",
                "UDD_PASSWORD": "udd",
            },
        ):
            source = UDDDataSource(table="packages", limit=1)
            source.retrieve()

            call_kwargs = mock_connect.call_args.kwargs
            assert call_kwargs["host"] == "192.168.1.218"
            assert call_kwargs["user"] == "udd"
            assert call_kwargs["password"] == "udd"
