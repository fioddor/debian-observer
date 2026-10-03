# SPDX-License-Identifier: AGPL-3.0-only
# Purpose: a) Container image for a python CLI application.
#          b) Production-ready container for the FastAPI analytical service.
# Author.: Igor Zubiaurre <igor@zubiaurre.es>
#-----------------------------------------------------------------------------

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definitions and source
COPY pyproject.toml pytest.ini mypy.ini README.md ./
COPY src/ ./src/

# Install the package
RUN pip install --no-cache-dir -e "."

# Create data directories
RUN mkdir -p /app/data/raw /app/data/parquet

# Expose API port
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8000/health || exit 1

# Run the API service using the CLI, which supports PARQUET_DIR env var
CMD ["debian-observer", "serve", "--host", "0.0.0.0", "--port", "8000"]
