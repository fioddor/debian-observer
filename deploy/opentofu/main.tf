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
  name       = "${var.project_name}-nats"
  image      = docker_image.nats.image_id
  privileged = true

  command = [
    "--jetstream",
    "--store_dir", "/data/jetstream",
    "--port", "14222",
    "--http_port", "18222",
  ]

  ports {
    internal = 14222
    external = var.nats_port
    ip       = "127.0.0.1"
  }

  ports {
    internal = 18222
    external = var.nats_monitoring_port
    ip       = "127.0.0.1"
  }

  volumes {
    container_path = "/data/jetstream"
    host_path      = abspath("${var.data_dir}/nats")
  }

  networks_advanced {
    name = docker_network.observatory.name
  }
}

# ---------------------------------------------------------------------------
# Debian Observer API
# ---------------------------------------------------------------------------

resource "docker_image" "observer" {
  name = var.observer_image
  build {
    context = "../.."
    dockerfile = "deploy/compose/Dockerfile"
  }
}

resource "docker_container" "observer_api" {
  name  = "${var.project_name}-api"
  image = docker_image.observer.image_id

  env = [
    "NATS_URL=nats://${docker_container.nats.name}:14222",
    "PARQUET_DIR=/data/parquet",
  ]

  ports {
    internal = 8000
    external = var.api_port
  }

  volumes {
    container_path = "/data/parquet"
    host_path      = abspath("${var.data_dir}/parquet")
  }

  networks_advanced {
    name = docker_network.observatory.name
  }

  depends_on = [docker_container.nats]
}
