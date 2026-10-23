#!/usr/bin/env python3
"""Example: the external packages consumer — the R1 queue consumer.

This is a RECIPE, not an installed command: the tool ships no
``consume`` CLI (the queue consumer is a pipeline you own —
"muere externalizado"). Copy this file into your own project,
adjust ``DURABLE`` / ``SUBJECT`` / ``DATASET_DIR``, and run it as
its own process::

    DEBIAN_OBSERVER_DATASET_DIR=data/normalized/package_versions \
      python examples/consume_packages.py

The queue state lives SERVER-SIDE in JetStream under the durable
name (plus the dataset table itself): ``kill -9`` the process and
JetStream redelivers the unacked messages — the consolidation
merge absorbs duplicates (idempotent fold), so the consumer
retoma where it left off. The ack happens only AFTER the durable
table write (the ConsumerApp wrapper acks when the handler
returns without exception — ADR-005): a crash before the ack means
redelivery, not loss.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from debian_observer.consumer_app import ConsumerApp
from debian_observer.storage.consolidation import (
    ConsolidationMerge,
    row_from_envelope,
)

# Adjust these to your deployment.
SERVERS = [os.environ.get("DEBIAN_OBSERVER_SERVER", "nats://localhost:4222")]
STREAM = os.environ.get("DEBIAN_OBSERVER_STREAM", "debian-events")
DURABLE = os.environ.get("DEBIAN_OBSERVER_DURABLE", "packages")
SUBJECT = os.environ.get(
    "DEBIAN_OBSERVER_SUBJECT", "debian.package_version.observed"
)
DATASET_DIR = Path(
    os.environ.get(
        "DEBIAN_OBSERVER_DATASET_DIR", "data/normalized/package_versions"
    )
)

merge = ConsolidationMerge()


def handle(envelope: Any) -> None:
    """Feed one observed event into the merge and rewrite the table.

    The durable write happens BEFORE the ack (the ConsumerApp
    wrapper acks after this returns): a crash mid-handler ⇒
    JetStream redelivers ⇒ the merge absorbs the duplicate.
    """
    row = row_from_envelope(envelope)
    merge.feed(row)
    merge.write(DATASET_DIR)  # atomic rewrite — the durable step


app = ConsumerApp()
app.register_handler("PackageVersionObserved", handle)


if __name__ == "__main__":
    asyncio.run(
        app.run(
            servers=SERVERS, stream=STREAM, subject=SUBJECT, durable=DURABLE
        )
    )