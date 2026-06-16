terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

locals {
  project_id = "my-gcp-project"
  region     = "europe-west1"
  name       = "outboxer"
  image      = "ghcr.io/fvdsn/outboxer:v0.1.0"

  labels = {
    app = local.name
  }

  env = {
    HEALTH_PORT          = "8080"
    LOG_FORMAT           = "json"
    PUBSUB_ENABLED       = "true"
    PUBSUB_PROJECT_ID    = local.project_id
    DEFAULT_PUBSUB_TOPIC = "events"
    PG_HOST              = "10.0.0.5"
    PG_USER              = "outboxer"
    PG_DATABASE          = "app"
    PG_SSL               = "true"
  }

  secret_env = {
    PG_PASSWORD = {
      secret  = "outboxer-db-password"
      version = "latest"
    }
  }
}

provider "google" {
  project = local.project_id
  region  = local.region
}

resource "google_service_account" "outboxer" {
  account_id   = local.name
  display_name = "Outboxer Cloud Run"
}

resource "google_project_iam_member" "pubsub_publisher" {
  project = local.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.outboxer.email}"
}

resource "google_secret_manager_secret_iam_member" "secret_access" {
  for_each  = toset([for value in values(local.secret_env) : value.secret])
  project   = local.project_id
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.outboxer.email}"
}

resource "google_cloud_run_v2_service" "outboxer" {
  name                = local.name
  location            = local.region
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = false
  labels              = local.labels

  template {
    service_account                  = google_service_account.outboxer.email
    timeout                          = "300s"
    max_instance_request_concurrency = 1
    labels                           = local.labels

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      image = local.image

      ports {
        container_port = 8080
      }

      resources {
        cpu_idle = false
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }

      dynamic "env" {
        for_each = local.env
        content {
          name  = env.key
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.secret_env
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = env.value.secret
              version = env.value.version
            }
          }
        }
      }
    }
  }

  depends_on = [
    google_project_iam_member.pubsub_publisher,
    google_secret_manager_secret_iam_member.secret_access,
  ]
}

output "service_uri" {
  value = google_cloud_run_v2_service.outboxer.uri
}
