variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "debian-observatory"
}

variable "network_name" {
  description = "Docker network name"
  type        = string
  default     = "observatory-net"
}

variable "data_dir" {
  description = "Host directory for persistent data"
  type        = string
  default     = "./data"
}

variable "nats_version" {
  description = "NATS server version tag"
  type        = string
  default     = "2.10"
}

variable "nats_port" {
  description = "External NATS client port"
  type        = number
  default     = 4222
}

variable "nats_monitoring_port" {
  description = "External NATS monitoring port"
  type        = number
  default     = 8222
}

variable "observer_image" {
  description = "Debian Observer Docker image"
  type        = string
  default     = "debian-observer:latest"
}

variable "api_port" {
  description = "External API port"
  type        = number
  default     = 8000
}
