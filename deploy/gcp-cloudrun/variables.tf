variable "project_id" {
  description = "GCP project that hosts the ephemeral test deployment."
  type        = string
}

variable "region" {
  description = "Region for every regional resource. Pub/Sub ordering requires the relay to publish through this region's endpoint."
  type        = string
  default     = "europe-west1"
}

variable "image" {
  description = "Fully qualified Outboxer container image in Artifact Registry. Pushed by the up recipe between the bootstrap and full applies."
  type        = string
  default     = ""
}

variable "operator_member" {
  description = "IAM member running the test harness (invoker on the service, subscriber on the subscription). Defaults to the caller's ADC identity."
  type        = string
  default     = ""
}

# Sizing defaults are deliberately realistic (not minimal): the stack is
# ephemeral, so it is paid by the hour, and the tests include performance
# measurement. Roughly $0.55/hour while up.

variable "cloudsql_tier" {
  description = "Cloud SQL machine tier."
  type        = string
  default     = "db-custom-4-16384"
}

variable "cloudsql_disk_gb" {
  description = "Cloud SQL SSD size in GB. Disk size also scales IOPS, so it is sized for throughput, not data."
  type        = number
  default     = 100
}

variable "run_cpu" {
  description = "Cloud Run CPU limit for the relay."
  type        = string
  default     = "2"
}

variable "run_memory" {
  description = "Cloud Run memory limit for the relay."
  type        = string
  default     = "1Gi"
}

variable "collect_batch_target" {
  description = "Relay COLLECT_BATCH_TARGET."
  type        = number
  default     = 5000
}
