# OpenTofu Deployment

Declarative infrastructure for Debian Observer using OpenTofu.

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

## Variables

Defaults in `variables.tf`: `project_name` (`debian-observatory`),
`network_name` (`observatory-net`), `data_dir` (`./data`),
`nats_version` (`2.10`), `nats_port` (`14222`),
`nats_monitoring_port` (`18222`), `observer_image`
(`debian-observer:latest`), `api_port` (`18000`).

## Outputs

| Output | Description |
|--------|-------------|
| `nats_url` | NATS client connection URL |
| `nats_monitoring_url` | NATS HTTP monitoring endpoint |
| `api_url` | Debian Observer API endpoint |
| `network_name` | Docker network name |

## Destroy

```bash
tofu destroy
```