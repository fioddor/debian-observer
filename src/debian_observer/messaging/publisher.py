"""Messaging: Event publisher.

Publishes ``EventEnvelope`` instances to NATS JetStream streams
with correct subject routing based on event type.

Design notes
------------

* The publisher does not create streams. Callers must ensure the
  stream exists before publishing (see ``StreamConfig`` +
  ``NATSClient`` — or use ``ensure_stream`` below, the wrapper the
  publish CLI uses).
* Publishing is fire-and-forget at the JetStream level. JetStream
  handles durability and retry internally.
* ``headers`` pass through to ``js.publish`` — the publish CLI sends
  ``Nats-Msg-Id: <event_id>`` on every message so JetStream
  deduplicates server-side within the stream's duplicate window
  (idempotent republish — ADR-004 amendment, 2026-10-23). The most
  recent ``PubAck`` is kept on ``last_ack`` (it carries ``duplicate``
  when the server reports it).
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
        #: The most recent ``PubAck`` from the last ``publish`` call
        #: (carries ``seq``; ``duplicate`` when the server reports it).
        self.last_ack: Any = None

    async def publish(
        self,
        event: EventEnvelope,
        *,
        headers: dict[str, str] | None = None,
    ) -> int | None:
        """Publish a single event to JetStream.

        Args:
            event: The event envelope to publish.
            headers: Optional NATS message headers (e.g.
                ``{"Nats-Msg-Id": str(event.event_id)}`` for JetStream
                server-side deduplication).

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
                ack = await js.publish(subject, data, headers=headers)
                self.last_ack = ack
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


async def ensure_stream(client: Any, stream_config: StreamConfig) -> Any:
    """Ensure the JetStream stream exists (ADR-004 topology).

    Looks the stream up first; creates it with the declarative
    ``StreamConfig`` (subjects, storage, retention, limits) only
    when missing. Returns the stream info (or the add_stream
    result for a freshly created stream).

    Usage::

        client = NATSClient(servers=["nats://localhost:4222"])
        await client.connect()
        await ensure_stream(client, StreamConfig(stream_name="debian-events"))
    """
    js = client.jetstream
    name = stream_config.stream_name
    try:
        return await js.stream_info(name)
    except Exception:
        # NotFoundError (or the fake's stand-in) → create below;
        # any other failure resurfaces from add_stream.
        pass
    return await js.add_stream(**stream_config.to_nats_config())
