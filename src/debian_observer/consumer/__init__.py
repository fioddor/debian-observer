"""Consumer layer.

Local consumers over the append-only event log: R1's "message queue"
is the Parquet event log on disk (ADR-003 amendment, 2026-10-23) —
no NATS/streaming is involved.
"""