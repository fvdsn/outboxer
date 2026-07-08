variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "eu-central-1"
}

# Sizing matches the other stacks: realistic by default, paid by the hour.

variable "rds_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.m7g.xlarge" # 4 vCPU / 16 GB
}

variable "rds_storage_gb" {
  description = "RDS gp3 storage in GB."
  type        = number
  default     = 100
}
