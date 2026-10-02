"""Messaging: Event publisher.

Publishes ``EventEnvelope`` instances to NATS JetStream streams
with correct subject routing based on event type.

Design notes
------------

* The publisher does not create streams. Callers must ensure the
  stream exists before publishing (see ``StreamConfig`` + ``NATSClient``).
* Publishing is fire-and-forget at the JetStream level. JetStream
  handles durability and retry internally.
* Subject routing follows the mapping defined in ``StreamConfig``.
"""

from __future__ import annotations

from typing import Any

from opentelemetry import trace

from debian_observer.events import EventEnvelope
from debian_observer.messaging.config import StreamConfig
from debian_observer.observability.metrics import MessagingMetrics
from debian_observer.observability.tracing import get_tracer

# ---------------------------------------------------------------------------
# Event publisher
# ---------------------------------------------------------------------------


class EventPublisher:
    """Publishes domain events to NATS JetStream.

    The publisher routes events to the correct NATS subject based on
    ``event_type``, serializes the envelope to JSON bytes, and sends
    it via JetStream.

    Usage::

        publisher = EventPublisher(client=nats_client)
        seq = await publisher.publish(event)
        print(f"Published to sequence {seq}")
    """

    def __init__(
        self,
        *,
        client: Any,
        stream_config: StreamConfig | None = None,
    ) -> None:
        """Initialize the publisher.

        Args:
            client: A connected ``NATSClient`` instance.
            stream_config: Optional stream configuration. Uses defaults
                           if not provided.
        """
        self._client = client
        self._config = stream_config or StreamConfig()
        self._metrics = MessagingMetrics()
        self._tracer = get_tracer("debian_observer.messaging")

    async def publish(self, event: EventEnvelope) -> int | None:
        """Publish a single event to JetStream.

        Args:
            event: The event envelope to publish.

        Returns:
            The JetStream sequence number, or None if publish failed.

        Raises:
            RuntimeError: If the NATS client is not connected.
            ValueError: If the event type has no mapped subject.
        """
        if not self._client.is_connected:
            raise RuntimeError("NATS client not connected")

        subject = self._config.subject_for(event.event_type)
        if subject is None:
            raise ValueError(f"No subject mapping for event type: {event.event_type}")

        with self._tracer.start_as_current_span("messaging.publish") as span:
            span.set_attribute("messaging.system", "NATS")
            span.set_attribute("messaging.destination", subject)
            span.set_attribute("event.type", event.event_type)

            js = self._client.jetstream
            data = event.to_json_bytes()
            try:
                ack = await js.publish(subject, data)
                seq = ack.seq if hasattr(ack, "seq") else None
                self._metrics.add_published(
                    1, subject=subject, event_type=event.event_type
                )
                span.set_attribute("messaging.sequence", seq)
                return seq
            except Exception as exc:
                self._metrics.add_failed(
                    1, subject=subject, event_type=event.event_type
                )
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
                raise

    async def publish_batch(self, events: list[EventEnvelope]) -> list[int | None]:
        """Publish a batch of events sequentially.

        Note: This is a simple sequential loop. For high-volume
        scenarios, consider concurrent publishing or NATS batch APIs.

        Args:
            events: List of event envelopes to publish.

        Returns:
            List of sequence numbers (or None for failures).
        """
        results: list[int | None] = []
        for event in events:
            seq = await self.publish(event)
            results.append(seq)
        return results
