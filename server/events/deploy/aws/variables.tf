variable "region" {
  type = string
}

variable "name" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,17}$", var.name))
    error_message = "Use 1 to 18 lowercase letters, digits, or hyphens, starting with a letter."
  }
}

variable "cluster_id" {
  type = string
  validation {
    condition     = can(regex("^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$", var.cluster_id)) && var.cluster_id != "00000000-0000-0000-0000-000000000000"
    error_message = "Use a stable, nonzero Events cluster UUID."
  }
}

variable "vpc_id" {
  type = string
}

variable "peer_zone_id" {
  type        = string
  description = "Private Route 53 hosted zone associated with the VPC."
}

variable "client_zone_id" {
  type        = string
  description = "Route 53 zone containing the name on the ACM certificate."
}

variable "client_dns_name" {
  type = string
}

variable "client_certificate_arn" {
  type        = string
  description = "Regional ACM certificate covering client_dns_name."
}

variable "ingress_subnet_ids" {
  type        = set(string)
  description = "Load balancer subnets covering every active broker zone."
}

variable "internal_ingress" {
  type    = bool
  default = true
}

variable "client_ipv4_cidrs" {
  type = set(string)
  validation {
    condition     = length(var.client_ipv4_cidrs) > 0 && alltrue([for cidr in var.client_ipv4_cidrs : can(cidrnetmask(cidr))])
    error_message = "Provide at least one IPv4 client network."
  }
}

variable "service_ipv4_cidrs" {
  type        = set(string)
  description = "Outbound TCP/443 target ranges for S3, Secrets Manager, KMS and SSM through VPC endpoints or NAT."
  validation {
    condition     = length(var.service_ipv4_cidrs) > 0 && alltrue([for cidr in var.service_ipv4_cidrs : can(cidrnetmask(cidr))])
    error_message = "Provide the IPv4 networks used to reach AWS service endpoints."
  }
}

variable "artifact_bucket" {
  type = string
}

variable "backup_bucket" {
  type        = string
  description = "Existing versioned, encrypted object storage bucket."
}

variable "backup_prefix" {
  type = string
  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._/-]*[A-Za-z0-9]$", var.backup_prefix))
    error_message = "Use a dedicated S3 backup prefix without a leading or trailing slash."
  }
}

variable "kms_key_arns" {
  type        = set(string)
  description = "Customer-managed keys used by the configuration secrets and artifact objects, if any."
  default     = []
}

variable "backup_kms_key_arn" {
  type        = string
  description = "KMS key for SSE-KMS on the backup bucket."
}

variable "disk_kms_key_arn" {
  type        = string
  description = "Optional KMS key for the broker data disks. The regional EBS default applies if empty."
  default     = ""
}

variable "metadata_voters" {
  type        = set(number)
  description = "Bootstrap metadata voters. The live group changes through Events operator operations."
  validation {
    condition     = length(var.metadata_voters) == 3 && alltrue([for id in var.metadata_voters : id >= 1 && id == floor(id)])
    error_message = "Use three distinct, positive integer bootstrap voter IDs."
  }
}

variable "brokers" {
  type = map(object({
    subnet_id           = string
    ami_id              = string
    instance_type       = string
    secret_arn          = string
    artifact_key        = string
    artifact_sha256     = string
    disk_gib            = number
    disk_iops           = optional(number, 3000)
    disk_throughput_mib = optional(number, 125)
    provision_instance  = optional(bool, true)
    start_broker        = optional(bool, true)
    format_blank_volume = optional(bool, false)
  }))
  validation {
    condition = alltrue([
      for broker_id, broker in var.brokers :
      can(regex("^[1-9][0-9]{0,9}$", broker_id)) &&
      broker.disk_gib >= 4 && broker.disk_gib == floor(broker.disk_gib) &&
      can(regex("^[0-9a-f]{64}$", broker.artifact_sha256)) &&
      length(broker.artifact_key) > 0 && !startswith(broker.artifact_key, "/") &&
      length(regexall("[*?]", broker.artifact_key)) == 0
    ])
    error_message = "Use positive integer IDs, disks of at least 4 GiB, exact S3 keys, and lowercase SHA-256 digests."
  }
}
