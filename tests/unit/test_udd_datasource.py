"""Tests for UDDDataSource."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

# Ensure psycopg2 is mocked before import
sys.modules["psycopg2"] = MagicMock()

from debian_observer.ingestion.acquisition import UDDDataSource


class TestUDDDataSource:
    """Unit tests for the UDD PostgreSQL data source."""

    @patch("psycopg2.connect")
    def test_retrieve_queries_database(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("hello", "2.10-3", "sid"),
            ("world", "1.0", "trixie"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="sources", limit=5)
        records = source.retrieve()

        assert len(records) == 2
        assert records[0]["package"] == "hello"
        assert records[0]["distribution"] == "sid"
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "sources" in sql
        assert "LIMIT" in sql

    @patch("psycopg2.connect")
    def test_compute_checksum_deterministic(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("hello", "2.10-3", "sid"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="sources")
        checksum1 = source.compute_checksum()
        checksum2 = source.compute_checksum()
        assert checksum1 == checksum2
        assert len(checksum1) == 64
