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
curl http://localhost:8000/health

# List metrics
curl http://localhost:8000/metrics/
```

### Stop

```bash
docker compose down
```

## Data Volumes

| Volume | Purpose |
|--------|---------|
| `nats-data` | JetStream persistence |
| `api-data` | API service data directory (raw artifacts, Parquet files) |

## Infrastructure as Code (OpenTofu)

The `opentofu/` directory is reserved for future Terraform/OpenTofu configurations.
R1 focuses on local deployment; cloud mappings are documented in the architecture spec.
