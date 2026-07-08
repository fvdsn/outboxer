variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "eu-central-1"
}

variable "image" {
  description = "Fully qualified Outboxer image in ECR. Pushed by the up recipe between the bootstrap and full applies."
  type        = string
  default     = ""
}

variable "deploy_relay" {
  description = "Whether to create the relay service. The up recipe applies with false first, runs the schema init task, then flips to true — the relay fails fast without a schema."
  type        = bool
  default     = false
}

# Sizing matches the GCP stacks: realistic by default, paid by the hour.

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

variable "task_cpu" {
  description = "Fargate task CPU units (2048 = 2 vCPU)."
  type        = string
  default     = "2048"
}

variable "task_memory" {
  description = "Fargate task memory in MiB (2 vCPU requires at least 4096)."
  type        = string
  default     = "4096"
}

variable "collect_batch_target" {
  description = "Relay COLLECT_BATCH_TARGET."
  type        = number
  default     = 5000
}

variable "sqs_send_concurrency" {
  description = "Relay SQS_SEND_CONCURRENCY: concurrent SendMessageBatch calls. SQS caps batches at 10 messages, so this is the AWS throughput knob."
  type        = number
  default     = 8
}
