# Deployment

Debian Observer R1 deployment configurations.

## Local Development (Docker Compose)

The `compose/` directory contains a Docker Compose setup for local deployment:

- **NATS JetStream** — Event backbone with durable streams
- **API Service** — FastAPI analytical endpoints

### Quick Start

```bash
cd deploy/compose
docker compose up -d
```

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
docker compose down
```

## Ports

| Port | Service |
|--------|---------|
| 4222 | NATS JetStream |
| 8222 | NATS monitoring |
| 8088 | API (changed from 8000 on 2026-10-26: collided with nginx-proxy on the deployment host) |

## Data Volumes

| Volume | Purpose |
|--------|---------|
| `nats-data` | JetStream persistence |
| `api-data` | API service data directory (raw artifacts, Parquet files) |

## Infrastructure as Code (OpenTofu)

The `opentofu/` directory is reserved for future Terraform/OpenTofu configurations.
R1 focuses on local deployment; cloud mappings are documented in the architecture spec.
