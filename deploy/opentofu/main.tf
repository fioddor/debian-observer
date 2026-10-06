# OpenTofu configuration for Debian Observatory R1
# Deploys NATS JetStream + app services via Docker Compose

terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

resource "docker_network" "observatory" {
  name   = var.network_name
  driver = "bridge"
}

# ---------------------------------------------------------------------------
# NATS JetStream
# ---------------------------------------------------------------------------

resource "docker_image" "nats" {
  name         = "nats:${var.nats_version}"
  keep_locally = true
}

resource "docker_container" "nats" {
  name  = "${var.project_name}-nats"
  image = docker_image.nats.image_id

  command = [
    "--jetstream",
    "--store_dir", "/data/jetstream",
  ]

  ports {
    internal = 4222
    external = var.nats_port
  }

  ports {
    internal = 8222
    external = var.nats_monitoring_port
  }

  volumes {
    container_path = "/data/jetstream"
    host_path      = "${var.data_dir}/nats"
  }

  networks_advanced {
    name = docker_network.observatory.name
  }

  healthcheck {
    test     = ["CMD", "wget", "-qO-", "http://localhost:8222/healthz"]
    interval = "10s"
    timeout  = "5s"
    retries  = 5
  }
}

# ---------------------------------------------------------------------------
# Debian Observer API
# ---------------------------------------------------------------------------

resource "docker_image" "observer" {
  name         = var.observer_image
  keep_locally = false
}

resource "docker_container" "observer_api" {
  name  = "${var.project_name}-api"
  image = docker_image.observer.image_id

  env = [
    "NATS_URL=nats://${docker_container.nats.name}:4222",
    "PARQUET_DIR=/data/parquet",
  ]

  ports {
    internal = 8000
    external = var.api_port
  }

  volumes {
    container_path = "/data/parquet"
    host_path      = "${var.data_dir}/parquet"
  }

  networks_advanced {
    name = docker_network.observatory.name
  }

  depends_on = [docker_container.nats]
}
