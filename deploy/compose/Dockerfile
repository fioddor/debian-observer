# SPDX-License-Identifier: AGPL-3.0-or-later
# Purpose: a) Container image for a python CLI application.
#          b) Production-ready container for the FastAPI analytical service.
# Author.: Igor Zubiaurre <igor@zubiaurre.es>
#-----------------------------------------------------------------------------

FROM python:3.11-slim-trixie

WORKDIR /app

# Install system dependencies (minimal)
# postgresql-client: pg_restore for `ingest udddump` (ADR-012). The
# base is pinned to trixie: the floating 3.11-slim tag can resolve to
# a cached bookworm-era image whose pg_restore 15 cannot read the
# UDD custom dump (its header pins vmaj 16); trixie ships 17.
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definitions and source
COPY pyproject.toml pytest.ini mypy.ini README.md ./
COPY src/ ./src/

# Install the package
RUN pip install --no-cache-dir -e "."

# Create data directories
RUN mkdir -p /app/data/raw /app/data/parquet

# Expose API port (8088 since 2026-10-26: 8000 collided with nginx-proxy
# on the deployment host; the CLI default matches — serve.py)
EXPOSE 8088

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8088/health || exit 1

# Run the API service using the CLI, which supports PARQUET_DIR env var
CMD ["debian-observer", "serve", "--host", "0.0.0.0", "--port", "8088"]
