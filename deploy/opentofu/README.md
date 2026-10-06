# OpenTofu Deployment — Debian Observatory R1

Declarative infrastructure for Debian Observatory using OpenTofu.

## Prerequisites

- [OpenTofu](https://opentofu.org/) >= 1.6
- Docker Engine
- `kreuzwerker/docker` provider (auto-installed)

## Quick Start

```bash
cd deploy/opentofu
tofu init
tofu plan
tofu apply
```

## Outputs

| Output | Description |
|--------|-------------|
| `nats_url` | NATS client connection URL |
| `nats_monitoring_url` | NATS HTTP monitoring endpoint |
| `api_url` | Debian Observer API endpoint |

## Destroy

```bash
tofu destroy
```
