# Deployment

Deployment configurations for Debian Observer.

## Local Development (Docker Compose)

The `compose/` directory contains a Docker Compose setup for local
deployment. Usage:
`docker compose -f deploy/compose/docker-compose.yml up -d`.

Services:

- **NATS JetStream** — event backbone with durable streams
- **API Service** — FastAPI analytical endpoints on port 8088
- **CLI** — `debian-observer` (container `debian-observer`),
  bind-mounts the repo at `/app` and pipx-installs a wheel from
  `dist/`; it needs the wheel built into `dist/` first
  (see docs/CONTRIBUTING.md, "Building/Packaging")

### Verify

```bash
# Check NATS health
curl http://localhost:8222/healthz

# Check API health
curl http://localhost:8088/health

# List metrics
curl http://localhost:8088/metrics/
```

### Stop

```bash
docker compose -f deploy/compose/docker-compose.yml down
```

## Ports

| Port | Service |
|--------|---------|
| 4222 | NATS JetStream |
| 8222 | NATS monitoring |
| 8088 | API |

## Data Volumes

| Volume | Purpose |
|--------|---------|
| `nats-data` | JetStream persistence |
| `api-data` | API service data directory (raw artifacts, Parquet files) |

## Infrastructure as Code (OpenTofu)

The `opentofu/` directory holds a declarative deployment of the same
stack (Docker network, NATS, observer image, API) as an alternative
to Docker Compose. See [opentofu/README.md](opentofu/README.md) and
the decision matrix in
[docs/CONTRIBUTING.md](../docs/CONTRIBUTING.md)
("Deployment: OpenTofu vs Docker Compose").