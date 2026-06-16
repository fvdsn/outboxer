terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30"
    }
  }
}

locals {
  project_id = "my-gcp-project"
  region     = "europe-west1"
  name       = "outboxer"
  namespace  = "outboxer"
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
      secret_name = "outboxer-db"
      key         = "password"
    }
  }
}

provider "google" {
  project = local.project_id
  region  = local.region
}

# Configure the kubernetes provider for your GKE cluster in this stack.
provider "kubernetes" {}

resource "google_service_account" "outboxer" {
  account_id   = local.name
  display_name = "Outboxer GKE"
}

resource "google_project_iam_member" "pubsub_publisher" {
  project = local.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.outboxer.email}"
}

resource "google_service_account_iam_member" "workload_identity" {
  service_account_id = google_service_account.outboxer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[${local.namespace}/${local.name}]"
}

resource "kubernetes_namespace_v1" "outboxer" {
  metadata {
    name   = local.namespace
    labels = local.labels
  }
}

resource "kubernetes_service_account_v1" "outboxer" {
  metadata {
    name      = local.name
    namespace = local.namespace
    labels    = local.labels
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.outboxer.email
    }
  }

  depends_on = [kubernetes_namespace_v1.outboxer]
}

resource "kubernetes_deployment_v1" "outboxer" {
  metadata {
    name      = local.name
    namespace = local.namespace
    labels    = local.labels
  }

  spec {
    replicas = 1

    selector {
      match_labels = local.labels
    }

    template {
      metadata {
        labels = local.labels
      }

      spec {
        service_account_name = kubernetes_service_account_v1.outboxer.metadata[0].name

        container {
          name              = "outboxer"
          image             = local.image
          image_pull_policy = "IfNotPresent"

          port {
            name           = "http"
            container_port = 8080
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
              value_from {
                secret_key_ref {
                  name = env.value.secret_name
                  key  = env.value.key
                }
              }
            }
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }

          liveness_probe {
            http_get {
              path = "/"
              port = 8080
            }
            initial_delay_seconds = 10
            period_seconds        = 30
          }
        }
      }
    }
  }

  depends_on = [
    google_project_iam_member.pubsub_publisher,
    google_service_account_iam_member.workload_identity,
    kubernetes_namespace_v1.outboxer,
  ]
}
