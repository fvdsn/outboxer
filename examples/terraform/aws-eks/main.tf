terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30"
    }
  }
}

locals {
  aws_region                = "eu-west-1"
  name                      = "outboxer"
  namespace                 = "outboxer"
  image                     = "ghcr.io/fvdsn/outboxer:v0.1.0"
  cluster_oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLE"
  cluster_oidc_issuer       = "oidc.eks.eu-west-1.amazonaws.com/id/EXAMPLE"
  queue_url                 = "https://sqs.eu-west-1.amazonaws.com/123456789012/events"
  queue_arn                 = "arn:aws:sqs:eu-west-1:123456789012:events"

  labels = {
    app = local.name
  }

  tags = {
    Application = local.name
  }

  env = {
    HEALTH_PORT           = "8080"
    LOG_FORMAT            = "json"
    SQS_ENABLED           = "true"
    AWS_REGION            = local.aws_region
    DEFAULT_SQS_QUEUE_URL = local.queue_url
    PG_HOST               = "app.cluster-abcdefghijkl.eu-west-1.rds.amazonaws.com"
    PG_USER               = "outboxer"
    PG_DATABASE           = "app"
    PG_SSL                = "true"
  }

  secret_env = {
    PG_PASSWORD = {
      secret_name = "outboxer-db"
      key         = "password"
    }
  }
}

provider "aws" {
  region = local.aws_region
}

# Configure the kubernetes provider for your EKS cluster in this stack.
provider "kubernetes" {}

data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.cluster_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.cluster_oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.cluster_oidc_issuer}:sub"
      values   = ["system:serviceaccount:${local.namespace}:${local.name}"]
    }
  }
}

resource "aws_iam_role" "outboxer" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = local.tags
}

data "aws_iam_policy_document" "sqs" {
  statement {
    actions = [
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
    ]
    resources = [local.queue_arn]
  }
}

resource "aws_iam_role_policy" "sqs" {
  name   = "${local.name}-sqs"
  role   = aws_iam_role.outboxer.id
  policy = data.aws_iam_policy_document.sqs.json
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
      "eks.amazonaws.com/role-arn" = aws_iam_role.outboxer.arn
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
    aws_iam_role_policy.sqs,
    kubernetes_namespace_v1.outboxer,
  ]
}
