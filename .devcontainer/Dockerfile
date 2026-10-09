# SPDX-License-Identifier: AGPL-3.0-only
# Purpose: Container image for Debian Observer
# Design.: Python 3.13.
#          Provides Python, development tooling, and NATS CLI.
# Pending: Will be refined as the application entrypoint stabilizes.
# Authors: Igor Zubiaurre <igor@zubiaurre.es>
#-----------------------------------------------------------------------------

FROM mcr.microsoft.com/devcontainers/python:3.13-trixie

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    jq \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install NATS CLI (useful for debugging JetStream during development)
RUN curl -sfSL https://github.com/nats-io/natscli/releases/download/v0.1.5/nats-0.1.5-linux-amd64.zip -o /tmp/nats.zip \
    && apt-get update && apt-get install -y --no-install-recommends unzip \
    && unzip -j -o /tmp/nats.zip nats-0.1.5-linux-amd64/nats -d /usr/local/bin \
    && chmod +x /usr/local/bin/nats \
    && rm /tmp/nats.zip

# Create vscode user (devcontainer standard) if not already present
RUN id -u vscode >/dev/null 2>&1 || useradd -m -s /bin/bash vscode

# Switch to vscode user for pip installations
USER vscode
WORKDIR /workspace

# Pre-install development dependencies for faster container startup
COPY --chown=vscode:vscode pyproject.toml README.md ./
RUN pip install --user --upgrade pip \
    && pip install --user -e ".[dev]"

# Default command
CMD ["sleep", "infinity"]
