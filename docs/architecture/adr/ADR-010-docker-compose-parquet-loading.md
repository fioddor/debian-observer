# ADR-010: Docker Compose Integration with Env-Based Parquet Loading

## Status

Accepted — 2026-10-03

## Context

The Debian Observer R1 FastAPI service needs to run inside Docker Compose alongside NATS JetStream. The service must optionally load Parquet data at startup when pre-computed datasets are available.

Two options were considered for passing the Parquet directory to the containerized service:

1. **Mount host directory via docker-compose.yml volumes** — Mount a host path into the container and pass the path via `PARQUET_DIR` environment variable.
2. **Hardcode in Dockerfile** — Bake the path directly into the container image, making it less flexible.

Additionally, the Dockerfile's `CMD` could either:

- Use `uvicorn` directly with `--factory` (how it was originally)
- Use the `debian-observer serve` CLI (which supports `--parquet-dir` and `PARQUET_DIR` env)

## Decision

1. **Use `debian-observer serve` CLI in the Dockerfile CMD** instead of `uvicorn` directly. This ensures the container uses the same entry point as local development and inherits all CLI features (host/port config, Parquet loading, help text).

2. **Support `PARQUET_DIR` environment variable** in the `serve` command. When `--parquet-dir` is not explicitly provided, fall back to the `PARQUET_DIR` env var. This allows docker-compose.yml to configure data loading without modifying the Dockerfile or CLI args.

3. **Keep `api-data` volume in docker-compose.yml** for persistent data storage, but do not pre-mount host Parquet paths. The operator can populate `/app/data/parquet` via the volume or leave it empty for a stateless API.

## Consequences

- **Positive**: Single source of truth for service startup (the CLI). Dockerfile and local dev use the same path.
- **Positive**: Flexible deployment — operators can enable/disable Parquet loading by setting/unsetting `PARQUET_DIR` in docker-compose.yml or at runtime.
- **Negative**: Slightly more indirection — the Dockerfile no longer shows the exact uvicorn invocation. Documented in `deploy/README.md` as mitigation.
- **Neutral**: The `serve` CLI's default host (`127.0.0.1`) is overridden to `0.0.0.0` in the Dockerfile CMD for container networking.

## Related

- `deploy/compose/Dockerfile`
- `deploy/compose/docker-compose.yml`
- `src/debian_observer/cli/commands/serve.py`
- `tests/cli/test_serve.py`
- `tests/test_deployment.py`
