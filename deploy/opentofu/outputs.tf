output "nats_url" {
  description = "NATS client URL"
  value       = "nats://localhost:${var.nats_port}"
}

output "nats_monitoring_url" {
  description = "NATS monitoring HTTP URL"
  value       = "http://localhost:${var.nats_monitoring_port}"
}

output "api_url" {
  description = "Debian Observer API URL"
  value       = "http://localhost:${var.api_port}"
}

output "network_name" {
  description = "Docker network used by services"
  value       = docker_network.observatory.name
}
