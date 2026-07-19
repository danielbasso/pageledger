variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "x86 EC2 instance type; m7i-flex.large (8 GB) is free-tier eligible and fits the stack."
  type        = string
  default     = "m7i-flex.large"
}

variable "volume_size" {
  description = "Root EBS volume size in GB (gp3). Holds Docker images + Kafka/Postgres volumes."
  type        = number
  default     = 30
}

variable "allowed_ssh_cidr" {
  description = "Optional override for the SSH CIDR. Leave empty to auto-detect your current public IP on each apply."
  type        = string
  default     = ""
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key registered on the instance for access."
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}
