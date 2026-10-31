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
            ("hello", "hello", "2.10-3", "debian", "sid", "amd64", "devel", "optional", "main"),
            ("libhello", "hello", "1.0", "debian", "trixie", "arm64", "libs", "optional", "main"),
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
            ("hello", "hello", "2.10-3", "debian", "sid", "amd64", "devel", "optional", "main"),
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
        mock_cursor.fetchall.return_value = [("pkg", "pkg", "1.0", "debian", "sid", "amd64", "devel", "optional", "main")]
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

    @patch("psycopg2.connect")
    def test_uses_constructor_params_over_env_vars(
        self, mock_connect: MagicMock
    ) -> None:
        """Constructor params should override env vars."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("pkg", "pkg", "1.0", "debian", "sid", "amd64", "devel", "optional", "main")]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        with patch.dict(
            os.environ,
            {
                "UDD_HOST": "env-host",
                "UDD_PORT": "5432",
                "UDD_DATABASE": "udd",
                "UDD_USER": "env-user",
                "UDD_PASSWORD": "env-pass",
            },
        ):
            source = UDDDataSource(
                table="packages",
                limit=1,
                host="ctor-host",
                user="ctor-user",
                password="ctor-pass",
            )
            source.retrieve()

            call_kwargs = mock_connect.call_args.kwargs
            assert call_kwargs["host"] == "ctor-host"
            assert call_kwargs["user"] == "ctor-user"
            assert call_kwargs["password"] == "ctor-pass"


class TestUDDFullFactFields:
    """Full package-version fact fields must survive retrieval."""

    @patch("psycopg2.connect")
    def test_retrieve_includes_release_section_priority(
        self, mock_connect: MagicMock
    ) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "librte-dpdk",
                "dpdk",
                "25.11.3-1",
                "debian",
                "bullseye-security",
                "amd64",
                "libs",
                "optional",
                "main",
            ),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="packages", limit=5)
        records = source.retrieve()

        assert records[0]["package"] == "librte-dpdk"
        assert records[0]["source_package"] == "dpdk"
        assert records[0]["version"] == "25.11.3-1"
        assert records[0]["release"] == "bullseye-security"
        assert records[0]["section"] == "libs"
        assert records[0]["priority"] == "optional"


class TestUDDRecordShapeUniformity:
    """UDD records emit the uniform fact shape; suite roles are unknown."""

    @patch("psycopg2.connect")
    def test_record_carries_suite_none(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("hello", "hello", "2.10-3", "debian", "sid", "amd64", "devel", "optional", "main"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="packages", limit=5)
        records = source.retrieve()

        assert records[0]["suite"] is None


class TestUDDComponentField:
    """The archive component is a packages-table identity field."""

    @patch("psycopg2.connect")
    def test_retrieve_includes_component(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "librte-dpdk",
                "dpdk",
                "25.11.3-1",
                "debian",
                "bullseye-security",
                "amd64",
                "libs",
                "optional",
                "main",
            ),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="packages", limit=5)
        records = source.retrieve()

        assert records[0]["component"] == "main"
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "component" in sql


class TestUDDUnlimited:
    """Unlimited ingestion (limit=None) omits the LIMIT clause."""

    @patch("psycopg2.connect")
    def test_limit_none_omits_limit_clause(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        source = UDDDataSource(table="packages", limit=None)
        source.retrieve()

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" not in sql.upper()

    def test_limit_none_is_the_default(self) -> None:
        source = UDDDataSource(table="packages")

        assert source._limit is None
