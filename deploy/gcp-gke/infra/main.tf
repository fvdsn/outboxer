# Cloud infrastructure for the ephemeral Outboxer-on-GKE test deployment: an
# Autopilot cluster, Cloud SQL, a Pub/Sub topic with an ordered subscription,
# an Artifact Registry repository, and Workload Identity IAM. Everything that
# runs *inside* the cluster is deliberately NOT Terraform: the Kubernetes
# manifests in ../k8s are the reference artifact, applied with kubectl.
#
# Resource names carry a -gke suffix so this stack can coexist with
# deploy/gcp-cloudrun in the same project.
#
# Authentication uses Workload Identity Federation directly: IAM roles are
# granted to the Kubernetes service account's principal, no Google service
# account or key file involved.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  name = "outboxer-gke"

  # The Kubernetes service account the manifests create (namespace/name), as a
  # Workload Identity principal. IAM bindings on principals do not require the
  # KSA to exist yet, so ordering with kubectl is not a concern.
  workload_member = "principal://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/outboxer/sa/outboxer"

  labels = {
    app           = local.name
    outboxer-test = "true"
  }
}

resource "google_project_service" "apis" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "sqladmin.googleapis.com",
  ])

  service            = each.value
  disable_on_destroy = false
}

# --- Container image repository ----------------------------------------------

resource "google_artifact_registry_repository" "outboxer" {
  repository_id = local.name
  location      = var.region
  format        = "DOCKER"
  labels        = local.labels

  depends_on = [google_project_service.apis]
}

# --- GKE Autopilot cluster ----------------------------------------------------
# Autopilot: no node pools to manage, per-pod billing, Workload Identity on by
# default — the realistic managed-Kubernetes baseline.

resource "google_container_cluster" "outboxer" {
  name     = local.name
  location = var.region

  enable_autopilot    = true
  deletion_protection = false

  resource_labels = local.labels

  depends_on = [google_project_service.apis]
}

# --- Database ------------------------------------------------------------------

resource "random_password" "db" {
  length  = 32
  special = false
}

# Cloud SQL instance names cannot be reused for ~a week after deletion.
resource "random_id" "suffix" {
  byte_length = 3
}

resource "google_sql_database_instance" "outboxer" {
  name             = "${local.name}-${random_id.suffix.hex}"
  database_version = "POSTGRES_17"
  region           = var.region

  deletion_protection = false

  settings {
    tier              = var.cloudsql_tier
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_type         = "PD_SSD"
    disk_size         = var.cloudsql_disk_gb
    disk_autoresize   = false
    user_labels       = local.labels

    deletion_protection_enabled = false

    ip_configuration {
      # Public IP with zero authorized networks: reachable only through the
      # IAM-gated connectors (the cloud-sql-proxy sidecar in the pods, and the
      # test harness's local proxy).
      ipv4_enabled = true
    }

    insights_config {
      query_insights_enabled = true
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_sql_database" "outboxer" {
  name     = "outboxer"
  instance = google_sql_database_instance.outboxer.name
}

resource "google_sql_user" "outboxer" {
  name     = "outboxer"
  instance = google_sql_database_instance.outboxer.name
  password = random_password.db.result
}

# --- Pub/Sub --------------------------------------------------------------------

resource "google_pubsub_topic" "events" {
  name   = "${local.name}-events"
  labels = local.labels

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "events" {
  name   = "${local.name}-events-sub"
  topic  = google_pubsub_topic.events.id
  labels = local.labels

  enable_message_ordering = true
  ack_deadline_seconds    = 30
}

# --- Workload Identity permissions for the outboxer pods -----------------------
# The workload identity pool (PROJECT.svc.id.goog) comes into existence with
# the project's first WI-enabled cluster, and IAM rejects bindings on
# principals of a pool that does not exist yet — hence the explicit
# dependency on the cluster.

resource "google_pubsub_topic_iam_member" "workload_publisher" {
  topic  = google_pubsub_topic.events.id
  role   = "roles/pubsub.publisher"
  member = local.workload_member

  depends_on = [google_container_cluster.outboxer]
}

resource "google_project_iam_member" "workload_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = local.workload_member

  depends_on = [google_container_cluster.outboxer]
}

# --- Operator (test harness) permissions ----------------------------------------

data "google_client_openid_userinfo" "me" {}

locals {
  operator_member = var.operator_member != "" ? var.operator_member : "user:${data.google_client_openid_userinfo.me.email}"
}

resource "google_pubsub_subscription_iam_member" "operator_subscriber" {
  subscription = google_pubsub_subscription.events.id
  role         = "roles/pubsub.subscriber"
  member       = local.operator_member
}
