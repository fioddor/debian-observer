"""Standalone consumer entrypoint.

Provides a high-level application layer over :mod:`messaging.consumer`
that adds idempotent event processing and graceful lifecycle management.

Usage::

    app = ConsumerApp()
    app.register_handler("package.updated", my_handler)
    await app.run(
        servers=["nats://localhost:4222"],
        stream="EVENTS",
        subject="debian.observer.>",
        durable="consumer-01",
    )
"""

from __future__ import annotations

import asyncio
import signal
from typing import Any, Awaitable, Callable, TypeVar

from debian_observer.events.envelope import EventEnvelope
from debian_observer.idempotency import IdempotencyStore
from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.consumer import EventConsumer

Handler = Callable[[EventEnvelope], Any]
"""User handler: receives only the EventEnvelope."""


# ---------------------------------------------------------------------------
# ConsumerApp
# ---------------------------------------------------------------------------

class ConsumerApp:
    """Standalone consumer with idempotency and graceful shutdown.

    Wraps the lower-level :class:`messaging.consumer.EventConsumer` with:

    - Handler registration by event type
    - Idempotency checking before user handler invocation
    - Graceful shutdown on SIGTERM / SIGINT

    Attributes:
        idempotency_store: Tracks processed event IDs.
        handlers: Mapping of event types to handler callables.
        _shutdown_event: asyncio.Event signalling graceful stop.
    """

    def __init__(self, idempotency_store: IdempotencyStore | None = None) -> None:
        self.idempotency_store = idempotency_store or IdempotencyStore()
        self.handlers: dict[str, Handler] = {}
        self._shutdown_event = asyncio.Event()

    def register_handler(self, event_type: str, handler: Handler) -> None:
        """Register a handler for a specific event type.

        Args:
            event_type: Canonical event type string.
            handler: Callable that receives an :class:`EventEnvelope`.
        """
        self.handlers[event_type] = handler

    def process(self, envelope: EventEnvelope) -> bool:
        """Process a single event envelope idempotently.

        Args:
            envelope: The event to consume.

        Returns:
            ``True`` if consumed successfully or already processed.
            ``False`` if no handler is registered for this event type.
        """
        if self.idempotency_store.is_processed(envelope.event_id):
            return True

        handler = self.handlers.get(envelope.event_type)
        if handler is None:
            return False

        handler(envelope)
        self.idempotency_store.mark_processed(envelope.event_id)
        return True

    async def run(
        self,
        *,
        servers: list[str] | None = None,
        stream: str,
        subject: str,
        durable: str,
    ) -> None:
        """Start consuming events from NATS JetStream.

        Blocks until :meth:`shutdown` is called or a signal is received.

        Args:
            servers: NATS server URLs. Defaults to ["nats://localhost:4222"].
            stream: JetStream stream name.
            subject: NATS subject filter.
            durable: Durable consumer name (survives restarts).
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._shutdown_event.set)

        client = NATSClient(servers=servers)
        await client.connect()

        async def _wrapped_handler(
            msg: Any,
            envelope: EventEnvelope,
            payload: dict[str, Any],
            ack: Callable[[], Awaitable[None]],
        ) -> None:
            """Internal handler that applies idempotency before user handler."""
            if self.process(envelope):
                await ack()
            else:
                # No handler registered — ack to avoid redelivery loop.
                await ack()

        consumer = EventConsumer(
            client=client,
            handler=_wrapped_handler,
            stream_name=stream,
            durable_name=durable,
        )
        await consumer.subscribe(subject)

        # Block until shutdown signal.
        await self._shutdown_event.wait()

        await consumer.close()
        await client.close()

    def shutdown(self) -> None:
        """Signal the consumer to stop gracefully."""
        self._shutdown_event.set()
