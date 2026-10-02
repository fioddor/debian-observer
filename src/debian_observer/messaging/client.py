"""Messaging: NATS client.

Async connection management for NATS and JetStream.

The client wraps ``nats.aio.client.Client`` with:
- Automatic JetStream context initialization
- Connection state tracking
- Graceful disconnect with drain

Usage::

    client = NATSClient(servers=["nats://localhost:4222"])
    await client.connect()
    js = client.jetstream
    await client.close()
"""

from __future__ import annotations

from typing import Any

try:
    import nats.aio.client as nats_client
except ImportError:
    nats_client = None  # type: ignore

# ---------------------------------------------------------------------------
# NATS client wrapper
# ---------------------------------------------------------------------------


class NATSClient:
    """Async NATS client wrapper with JetStream support.

    Attributes:
        servers: List of NATS server URLs.
        is_connected: Whether the client currently holds an open connection.

    Usage::

        client = NATSClient()
        await client.connect()
        js = client.jetstream
        # ... use js for pub/sub ...
        await client.close()
    """

    def __init__(
        self,
        *,
        servers: list[str] | None = None,
        user: str | None = None,
        password: str | None = None,
        token: str | None = None,
    ) -> None:
        """Initialize the NATS client (does not connect yet).

        Args:
            servers: NATS server URLs. Defaults to ["nats://localhost:4222"].
            user: Optional username for authentication.
            password: Optional password for authentication.
            token: Optional authentication token.
        """
        self.servers = servers if servers is not None else ["nats://localhost:4222"]
        self._user = user
        self._password = password
        self._token = token

        self._nc: Any = None  # nats.aio.client.Client
        self._js: Any = None  # nats.js.client.JetStreamContext
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Return whether the client holds an active connection."""
        return self._connected

    @property
    def jetstream(self) -> Any:
        """Return the JetStream context (requires active connection).

        Raises:
            RuntimeError: If the client is not connected.
        """
        if not self._connected or self._js is None:
            raise RuntimeError("NATS client not connected. Call connect() first.")
        return self._js

    async def connect(self) -> None:
        """Establish connection to NATS and initialize JetStream.

        Raises:
            RuntimeError: If nats-py is not installed.
            ConnectionError: If the connection fails.
        """
        if nats_client is None:
            raise RuntimeError("nats-py is required. Install with: pip install nats-py")

        kwargs: dict[str, Any] = {"servers": self.servers}
        if self._user is not None:
            kwargs["user"] = self._user
        if self._password is not None:
            kwargs["password"] = self._password
        if self._token is not None:
            kwargs["token"] = self._token

        self._nc = nats_client.Client()
        await self._nc.connect(**kwargs)
        self._js = self._nc.jetstream()
        self._connected = True

    async def close(self) -> None:
        """Gracefully close the NATS connection.

        Drains pending messages before disconnecting.
        """
        if self._nc is not None:
            try:
                await self._nc.drain()
            except Exception:
                pass  # Drain may fail if already closed
            try:
                await self._nc.close()
            except Exception:
                pass
        self._nc = None
        self._js = None
        self._connected = False
