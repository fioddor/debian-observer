"""Unit tests for NATSClient with mocked nats-py.

These tests verify connection management, authentication, and lifecycle
without requiring a real NATS server.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from debian_observer.messaging.client import NATSClient


class TestNATSClientInit:
    """Tests for NATSClient constructor."""

    def test_init_default_servers(self) -> None:
        """Default servers should be localhost:4222."""
        client = NATSClient()
        assert client.servers == ["nats://localhost:4222"]

    def test_init_custom_servers(self) -> None:
        """Custom servers should override defaults."""
        client = NATSClient(servers=["nats://remote:4222"])
        assert client.servers == ["nats://remote:4222"]

    def test_init_custom_auth(self) -> None:
        """User, password, and token should be stored."""
        client = NATSClient(user="alice", password="secret", token="tok123")
        assert client._user == "alice"
        assert client._password == "secret"
        assert client._token == "tok123"

    def test_is_connected_property_false_before_connect(self) -> None:
        """is_connected should be False before connect()."""
        client = NATSClient()
        assert client.is_connected is False

    def test_is_connected_property_true_after_connect(self) -> None:
        """is_connected should be True after successful connect()."""
        client = NATSClient()
        client._connected = True
        assert client.is_connected is True


class TestNATSClientConnect:
    """Tests for NATSClient.connect()."""

    async def test_connect_raises_when_nats_not_installed(self) -> None:
        """Should raise RuntimeError when nats-py is unavailable."""
        with patch("debian_observer.messaging.client.nats_client", None):
            client = NATSClient()
            with pytest.raises(RuntimeError, match="nats-py is required"):
                await client.connect()

    async def test_connect_success(self) -> None:
        """Should connect and initialize JetStream context."""
        mock_nats_module = MagicMock()
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.jetstream = MagicMock(return_value=MagicMock())
        mock_nats_module.Client.return_value = mock_nc

        with patch("debian_observer.messaging.client.nats_client", mock_nats_module):
            client = NATSClient()
            await client.connect()

        mock_nats_module.Client.assert_called_once()
        mock_nc.connect.assert_awaited_once_with(servers=["nats://localhost:4222"])
        mock_nc.jetstream.assert_called_once()
        assert client.is_connected is True
        assert client.jetstream is not None

    async def test_connect_with_auth(self) -> None:
        """Should pass credentials to NATS connect."""
        mock_nats_module = MagicMock()
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.jetstream = MagicMock(return_value=MagicMock())
        mock_nats_module.Client.return_value = mock_nc

        with patch("debian_observer.messaging.client.nats_client", mock_nats_module):
            client = NATSClient(
                servers=["nats://auth-server:4222"],
                user="alice",
                password="secret",
                token="tok123",
            )
            await client.connect()

        mock_nc.connect.assert_awaited_once_with(
            servers=["nats://auth-server:4222"],
            user="alice",
            password="secret",
            token="tok123",
        )

    async def test_connect_propagates_connection_error(self) -> None:
        """ConnectionError from NATS should propagate."""
        mock_nats_module = MagicMock()
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock(side_effect=ConnectionError("refused"))
        mock_nats_module.Client.return_value = mock_nc

        with patch("debian_observer.messaging.client.nats_client", mock_nats_module):
            client = NATSClient()
            with pytest.raises(ConnectionError, match="refused"):
                await client.connect()


class TestNATSClientJetstream:
    """Tests for NATSClient.jetstream property."""

    def test_jetstream_raises_when_not_connected(self) -> None:
        """Should raise RuntimeError when not connected."""
        client = NATSClient()
        with pytest.raises(RuntimeError, match="not connected"):
            _ = client.jetstream

    def test_jetstream_raises_when_js_is_none(self) -> None:
        """Should raise RuntimeError when _js is None."""
        client = NATSClient()
        client._connected = True
        client._js = None
        with pytest.raises(RuntimeError, match="not connected"):
            _ = client.jetstream

    def test_jetstream_returns_context(self) -> None:
        """Should return the JetStream context when connected."""
        client = NATSClient()
        mock_js = MagicMock()
        client._js = mock_js
        client._connected = True
        assert client.jetstream is mock_js


class TestNATSClientClose:
    """Tests for NATSClient.close()."""

    async def test_close_drains_and_closes(self) -> None:
        """Should drain pending messages then close connection."""
        client = NATSClient()
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        client._nc = mock_nc
        client._js = MagicMock()
        client._connected = True

        await client.close()

        mock_nc.drain.assert_awaited_once()
        mock_nc.close.assert_awaited_once()
        assert client.is_connected is False
        assert client._nc is None
        assert client._js is None

    async def test_close_handles_drain_exception(self) -> None:
        """Drain exception should not propagate."""
        client = NATSClient()
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock(side_effect=RuntimeError("already closed"))
        mock_nc.close = AsyncMock()
        client._nc = mock_nc
        client._connected = True

        await client.close()

        mock_nc.drain.assert_awaited_once()
        mock_nc.close.assert_awaited_once()
        assert client.is_connected is False

    async def test_close_handles_close_exception(self) -> None:
        """Close exception should not propagate."""
        client = NATSClient()
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock(side_effect=RuntimeError("boom"))
        client._nc = mock_nc
        client._connected = True

        await client.close()

        mock_nc.drain.assert_awaited_once()
        mock_nc.close.assert_awaited_once()
        assert client.is_connected is False

    async def test_close_handles_both_exceptions(self) -> None:
        """Exceptions in both drain and close should not propagate."""
        client = NATSClient()
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock(side_effect=RuntimeError("drain fail"))
        mock_nc.close = AsyncMock(side_effect=RuntimeError("close fail"))
        client._nc = mock_nc
        client._connected = True

        await client.close()

        assert client.is_connected is False
        assert client._nc is None

    async def test_close_when_nc_is_none(self) -> None:
        """Closing an unconnected client should be a no-op."""
        client = NATSClient()
        client._nc = None
        client._connected = False

        await client.close()

        assert client.is_connected is False
