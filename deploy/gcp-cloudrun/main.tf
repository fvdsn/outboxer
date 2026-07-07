# Ephemeral, realistically sized Outboxer deployment on Cloud Run, used by the
# cloud integration tests (test/cloud/gcpcloudrun) and doubling as the
# reference Cloud Run setup. The whole stack is created per test run and
# destroyed afterwards; nothing here is meant to persist.
#
# Design choices:
# - Cloud SQL is reached through connectors only: Cloud Run uses the built-in
#   /cloudsql unix-socket mount (pgx treats a PG_HOST starting with "/" as a
#   socket directory), and the test harness uses cloud-sql-proxy. The instance
#   has no authorized networks, so no VPC, peering, or serverless VPC
#   connector is needed and IAM is the only way in.
# - The relay publishes through the regional Pub/Sub endpoint: real Pub/Sub
#   requires regional endpoints when publishing with ordering keys.
# - Everything carries the outboxer-test label so an orphaned stack can be
#   swept by label if the local Terraform state is ever lost.

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

data "google_client_openid_userinfo" "me" {}

locals {
  name            = "outboxer"
  operator_member = var.operator_member != "" ? var.operator_member : "user:${data.google_client_openid_userinfo.me.email}"

  labels = {
    app           = local.name
    outboxer-test = "true"
  }
}

resource "google_project_service" "apis" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "sqladmin.googleapis.com",
  ])

  service            = each.value
  disable_on_destroy = false
}

# --- Container image repository -------------------------------------------
# Created first (targeted apply) so the image can be pushed before the full
# apply creates the Cloud Run service that references it.

resource "google_artifact_registry_repository" "outboxer" {
  repository_id = local.name
  location      = var.region
  format        = "DOCKER"
  labels        = local.labels

  depends_on = [google_project_service.apis]
}

# --- Database ---------------------------------------------------------------

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "google_secret_manager_secret" "db_password" {
  secret_id = "${local.name}-db-password"
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "db_password" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = random_password.db.result
}

resource "google_sql_database_instance" "outboxer" {
  name             = "${local.name}-${random_id.suffix.hex}"
  database_version = "POSTGRES_17"
  region           = var.region

  # Ephemeral test infrastructure: destroy must always succeed.
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
      # Public IP with zero authorized networks: unreachable except through
      # the IAM-gated connectors (Cloud Run socket mount, cloud-sql-proxy).
      ipv4_enabled = true
    }

    insights_config {
      query_insights_enabled = true
    }
  }

  depends_on = [google_project_service.apis]
}

# Cloud SQL instance names cannot be reused for ~a week after deletion, so
# each stack gets a random suffix to keep repeated up/down cycles working.
resource "random_id" "suffix" {
  byte_length = 3
}

resource "google_sql_database" "outboxer" {
  name     = local.name
  instance = google_sql_database_instance.outboxer.name
}

resource "google_sql_user" "outboxer" {
  name     = local.name
  instance = google_sql_database_instance.outboxer.name
  password = random_password.db.result
}

# --- Pub/Sub ----------------------------------------------------------------

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

# --- Relay identity and permissions -----------------------------------------

resource "google_service_account" "relay" {
  account_id   = "${local.name}-relay"
  display_name = "Outboxer relay (ephemeral test stack)"
}

resource "google_pubsub_topic_iam_member" "relay_publisher" {
  topic  = google_pubsub_topic.events.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_project_iam_member" "relay_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_secret_manager_secret_iam_member" "relay_db_password" {
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.relay.email}"
}

# The harness pulls from the subscription and invokes the service as the
# operator identity.
resource "google_pubsub_subscription_iam_member" "operator_subscriber" {
  subscription = google_pubsub_subscription.events.id
  role         = "roles/pubsub.subscriber"
  member       = local.operator_member
}

# --- Relay configuration shared by the service and the init job -------------

locals {
  relay_env = {
    LOG_FORMAT           = "json"
    PUBSUB_ENABLED       = "true"
    PUBSUB_PROJECT_ID    = var.project_id
    PUBSUB_API_ENDPOINT  = "${var.region}-pubsub.googleapis.com:443"
    DEFAULT_PUBSUB_TOPIC = google_pubsub_topic.events.name
    DLQ_TABLE            = "outboxer_dead_letters"
    COLLECT_BATCH_TARGET = tostring(var.collect_batch_target)
    PG_HOST              = "/cloudsql/${google_sql_database_instance.outboxer.connection_name}"
    PG_USER              = google_sql_user.outboxer.name
    PG_DATABASE          = google_sql_database.outboxer.name
    PG_SSL               = "false" # unix socket; transport is Google's encrypted connector
  }
}

# --- Schema provisioning job -------------------------------------------------
# Runs `outboxer init --apply` once per stack (executed by the up recipe).

resource "google_cloud_run_v2_job" "init" {
  name                = "${local.name}-init"
  location            = var.region
  labels              = local.labels
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.relay.email

      containers {
        image = var.image
        args  = ["init", "--apply"]

        dynamic "env" {
          for_each = local.relay_env
          content {
            name  = env.key
            value = env.value
          }
        }

        env {
          name = "PG_PASSWORD"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.db_password.secret_id
              version = "latest"
            }
          }
        }

        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }
      }

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [google_sql_database_instance.outboxer.connection_name]
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_iam_member.relay_db_password,
    google_project_iam_member.relay_cloudsql_client,
    google_secret_manager_secret_version.db_password,
  ]
}

# --- The relay ----------------------------------------------------------------

resource "google_cloud_run_v2_service" "relay" {
  name                = local.name
  location            = var.region
  labels              = local.labels
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.relay.email

    # Outboxer is a single-active worker: exactly one instance, CPU always
    # allocated so the relay keeps processing between requests.
    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      image = var.image

      resources {
        limits = {
          cpu    = var.run_cpu
          memory = var.run_memory
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      dynamic "env" {
        for_each = local.relay_env
        content {
          name  = env.key
          value = env.value
        }
      }

      env {
        name = "PG_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password.secret_id
            version = "latest"
          }
        }
      }

      # Cloud Run injects PORT; Outboxer reads it for the health server, which
      # also satisfies the startup probe.

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.outboxer.connection_name]
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_iam_member.relay_db_password,
    google_project_iam_member.relay_cloudsql_client,
    google_pubsub_topic_iam_member.relay_publisher,
    google_secret_manager_secret_version.db_password,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "operator_invoker" {
  name     = google_cloud_run_v2_service.relay.name
  location = var.region
  role     = "roles/run.invoker"
  member   = local.operator_member
}
