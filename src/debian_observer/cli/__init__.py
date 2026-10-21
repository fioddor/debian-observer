"""CLI layer.

Operational commands for the Debian Observer.

Usage::

    debian-observer --help
    debian-observer ingest udd --table packages --raw-dir data/raw
    debian-observer validate data/raw/<artifact>.csv.xz
    debian-observer query "SELECT * FROM events"
"""