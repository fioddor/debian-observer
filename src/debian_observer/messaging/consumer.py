"""Messaging: Event consumer.

Consumes domain events from NATS JetStream with at-least-once semantics.

The consumer follows the R1 reliability model:

1. Receive an event from JetStream
2. Deserialize into EventEnvelope
3. Invoke the handler with (message, event, payload_dict, ack)
4. If handler succeeds, ack the message
5. If handler fails, nak the message for retry

Design notes
------------

* The consumer does not implement global ordering. Chronological
  ordering is reconstructed from domain timestamps.
* Exactly-once processing is not guaranteed. Handlers must be
  idempotent (see ``IngestionHandler`` for the standard pattern).
* The ``ack`` callback is passed to the handler so that the handler
  controls when to ack (after persistence, before processing, etc.).
  The default behavior (ack after successful handler completion) is
  provided by ``EventConsumer``.
"""

from __future__ import annotations

import json
from typing import Any, Awaitable, Callable

from opentelemetry import trace

from debian_observer.events import EventEnvelope
from debian_observer.observability.metrics import MessagingMetrics
from debian_observer.observability.tracing import get_tracer

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

EventHandler = Callable[
    [Any, EventEnvelope, dict[str, Any], Callable[[], Awaitable[None]]],
    Awaitable[None],
]
"""Handler signature: (msg, event_envelope, payload_dict, ack_callback)."""


# ---------------------------------------------------------------------------
# Event consumer
# ---------------------------------------------------------------------------


class EventConsumer:
    """Consumes events from a NATS JetStream stream.

    The consumer subscribes to a subject and invokes the registered
    handler for each message. If the handler succeeds, the message is
    acknowledged. If it fails, the message is negative-acknowledged
    (triggering JetStream redelivery).

    Usage::

        async def my_handler(msg, event, data, ack):
            print(f"Processing {event.event_type}")
            await persist(data)
            await ack()  # Ack only after persistence

        consumer = EventConsumer(
            client=nats_client,
            handler=my_handler,
            stream_name="debian.events",
            durable_name="storage-consumer",
        )
        await consumer.subscribe("debian.package_version.observed")
    """

    def __init__(
        self,
        *,
        client: Any,
        handler: EventHandler,
        stream_name: str,
        durable_name: str,
        max_deliver: int = 5,
    ) -> None:
        """Initialize the consumer.

        Args:
            client: A connected ``NATSClient`` instance.
            handler: Async handler function for processing events.
            stream_name: Name of the JetStream stream to consume from.
            durable_name: Durable consumer name (survives restarts).
            max_deliver: Maximum delivery attempts before dead-letter.
        """
        self._client = client
        self.handler = handler
        self.stream_name = stream_name
        self.durable_name = durable_name
        self.max_deliver = max_deliver
        self._subscription: Any = None
        self._metrics = MessagingMetrics()
        self._tracer = get_tracer("debian_observer.messaging")

    async def subscribe(self, subject: str) -> None:
        """Subscribe to a subject and begin consuming messages.

        Args:
            subject: NATS subject filter (e.g. "debian.package_version.observed").

        Raises:
            RuntimeError: If the NATS client is not connected.
        """
        if not self._client.is_connected:
            raise RuntimeError("NATS client not connected")

        js = self._client.jetstream
        self._subscription = await js.subscribe(
            subject,
            stream=self.stream_name,
            durable=self.durable_name,
            cb=self._on_message,
            max_deliver=self.max_deliver,
        )

    async def _on_message(self, msg: Any) -> None:
        """Internal callback for incoming NATS messages."""
        try:
            await self._process_message(msg)
        except Exception:
            # Let JetStream handle retries via max_deliver
            if hasattr(msg, "nak"):
                try:
                    await msg.nak()
                except Exception:
                    pass
            raise

    async def _process_message(self, msg: Any) -> None:
        """Process a single message: deserialize, invoke handler, ack."""
        import time

        start = time.perf_counter()
        # Deserialize the envelope
        data = json.loads(msg.data.decode("utf-8"))
        event = EventEnvelope.model_validate(data)

        with self._tracer.start_as_current_span("messaging.consume") as span:
            span.set_attribute("messaging.system", "NATS")
            span.set_attribute("event.type", event.event_type)
            span.set_attribute("event.id", str(event.event_id))

            # Prepare ack callback
            async def ack() -> None:
                if hasattr(msg, "ack"):
                    await msg.ack()

            # Invoke handler
            try:
                await self.handler(msg, event, event.payload, ack)
                duration = time.perf_counter() - start
                self._metrics.record_consumer_duration(
                    duration, event_type=event.event_type
                )
                self._metrics.add_consumed(1, event_type=event.event_type)
            except Exception as exc:
                self._metrics.add_failed(1, event_type=event.event_type)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
                raise

            # If handler succeeded without exception, ensure ack.
            # We always ack here even if the handler already acked —
            # duplicate acks are harmless in NATS (no-op for acked messages).
            await ack()

    async def unsubscribe(self) -> None:
        """Unsubscribe and stop consuming messages."""
        if self._subscription is not None:
            try:
                await self._subscription.unsubscribe()
            except Exception:
                pass
            self._subscription = None

    async def close(self) -> None:
        """Unsubscribe and release resources."""
        await self.unsubscribe()


# ---------------------------------------------------------------------------
# Standard ingestion handler with idempotency
# ---------------------------------------------------------------------------


class IngestionHandler:
    """Standard handler pattern with idempotency (at-least-once semantics).

    This handler provides a simplified interface for the most common
    consumer pattern: process an event, skip duplicates, ack after
    processing.

    Usage::

        async def storage_processor(event: EventEnvelope) -> None:
            row = event.to_parquet_row()
            write_to_parquet(row)

        handler = IngestionHandler(processor=storage_processor)
        await handler.handle(event, ack)
    """

    def __init__(
        self,
        *,
        processor: Callable[[EventEnvelope], Awaitable[None]],
    ) -> None:
        """Initialize the handler.

        Args:
            processor: Async function that processes an EventEnvelope.
        """
        self._processor = processor
        self._seen: set[str] = set()

    async def handle(
        self,
        event: EventEnvelope,
        ack: Callable[[], Awaitable[None]],
    ) -> None:
        """Handle an event with idempotency.

        Args:
            event: The event to process.
            ack: Async callback to acknowledge the message.

        Note:
            Duplicates (same event_id) are detected and skipped, but
            the ack is still called so that NATS does not redeliver.
        """
        event_id = str(event.event_id)

        if event_id in self._seen:
            # Duplicate — ack but do not re-process
            await ack()
            return

        # Process the event
        await self._processor(event)
        self._seen.add(event_id)

        # Ack after successful processing
        await ack()
