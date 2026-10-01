"""Messaging layer for Debian Observatory.

Durable event streaming via NATS JetStream.

This module provides:

- ``NATSClient`` — async connection management with retry logic
- ``EventPublisher`` — publishes ``EventEnvelope`` to JetStream streams
- ``EventConsumer`` — consumes events with at-least-once semantics
- ``StreamConfig`` — declarative stream configuration
- ``IngestionHandler`` — standard handler pattern with idempotency

Usage::

    from debian_observer.messaging import NATSClient, EventPublisher, StreamConfig

    config = StreamConfig()
    client = NATSClient(servers=["nats://localhost:4222"])
    await client.connect()

    publisher = EventPublisher(client=client)
    seq = await publisher.publish(event)

    consumer = EventConsumer(
        client=client,
        handler=my_handler,
        stream_name=config.stream_name,
        durable_name="my-consumer",
    )
    await consumer.subscribe("debian.package_version.observed")
"""

from __future__ import annotations

from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.config import StreamConfig
from debian_observer.messaging.consumer import EventConsumer, IngestionHandler
from debian_observer.messaging.publisher import EventPublisher

__all__ = [
    "EventConsumer",
    "EventPublisher",
    "IngestionHandler",
    "NATSClient",
    "StreamConfig",
]
