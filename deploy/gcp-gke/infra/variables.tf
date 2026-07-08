variable "project_id" {
  description = "GCP project that hosts the ephemeral test deployment."
  type        = string
}

variable "region" {
  description = "Region for every regional resource, including the Autopilot cluster."
  type        = string
  default     = "europe-west1"
}

variable "operator_member" {
  description = "IAM member running the test harness. Defaults to the caller's ADC identity."
  type        = string
  default     = ""
}

# Sizing matches deploy/gcp-cloudrun: realistic by default, paid by the hour.

variable "cloudsql_tier" {
  description = "Cloud SQL machine tier."
  type        = string
  default     = "db-custom-4-16384"
}

variable "cloudsql_disk_gb" {
  description = "Cloud SQL SSD size in GB (disk size scales IOPS)."
  type        = number
  default     = 100
}
