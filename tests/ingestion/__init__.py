"""Ingestion tests: the britney source.

Synthetic, deterministic fixtures only (docs/release3.project.v0.1.md §19):
the archive index is a fixed HTML listing, the snapshots are gzip
files served by an embedded local HTTP server, and every politeness
delay is an injected sleeper — the battery never sleeps for real and
never reaches release.debian.org.
"""