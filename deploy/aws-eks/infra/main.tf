# Cloud infrastructure for the ephemeral Outboxer-on-EKS test deployment: an
# EKS Auto Mode cluster, RDS PostgreSQL, a standard and a FIFO SQS queue, an
# ECR repository, and EKS Pod Identity IAM. Everything that runs *inside* the
# cluster is deliberately NOT Terraform: the Kubernetes manifests in ../k8s
# are the reference artifact, applied with kubectl.
#
# Resource names carry an -eks suffix so this stack can coexist with
# deploy/aws-fargate in the same account.
#
# Design notes:
# - EKS Auto Mode is the Autopilot analog: AWS manages nodes, scaling, and
#   core addons (including the Pod Identity agent); pods drive capacity.
# - Pod authentication uses EKS Pod Identity — the modern successor to IRSA:
#   an association maps the Kubernetes service account to an IAM role, no
#   OIDC provider setup, no key files.
# - Unlike Cloud SQL, RDS is plain TCP: pods connect directly to the endpoint
#   with TLS, no proxy sidecar. The RDS security group admits the VPC (for
#   the pods) and the operator's apply-time IP (for the test harness).
# - Same minimal-VPC shape as deploy/aws-fargate: two public subnets, no NAT
#   gateway; Auto Mode nodes get public IPs to reach ECR and SQS.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
    http = {
      source  = "hashicorp/http"
      version = ">= 3.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      app           = "outboxer-eks"
      outboxer-test = "true"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "http" "operator_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  name          = "outboxer-eks"
  operator_cidr = "${chomp(data.http.operator_ip.response_body)}/32"
}

# --- Networking ---------------------------------------------------------------

resource "aws_vpc" "outboxer" {
  cidr_block           = "10.91.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = local.name }
}

resource "aws_internet_gateway" "outboxer" {
  vpc_id = aws_vpc.outboxer.id
}

resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.outboxer.id
  cidr_block              = cidrsubnet(aws_vpc.outboxer.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = { Name = "${local.name}-public-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.outboxer.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.outboxer.id
  }
}

resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "rds" {
  name   = "${local.name}-rds"
  vpc_id = aws_vpc.outboxer.id

  ingress {
    description = "Postgres from the cluster's pods (whole VPC)"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.outboxer.cidr_block]
  }

  ingress {
    description = "Postgres from the operator (test harness)"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [local.operator_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Container image repository -------------------------------------------------

resource "aws_ecr_repository" "outboxer" {
  name         = local.name
  force_delete = true
}

# --- Database ---------------------------------------------------------------------

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "outboxer" {
  name       = local.name
  subnet_ids = aws_subnet.public[*].id
}

resource "aws_db_instance" "outboxer" {
  identifier     = local.name
  engine         = "postgres"
  engine_version = "17"

  instance_class    = var.rds_instance_class
  allocated_storage = var.rds_storage_gb
  storage_type      = "gp3"

  db_name  = "outboxer"
  username = "outboxer"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.outboxer.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true

  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 0
  apply_immediately       = true

  performance_insights_enabled = true
}

# --- Queues -----------------------------------------------------------------------

resource "aws_sqs_queue" "events" {
  name = "${local.name}-events"
}

resource "aws_sqs_queue" "events_fifo" {
  name                        = "${local.name}-events.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
}

# --- EKS Auto Mode cluster ----------------------------------------------------------

data "aws_iam_policy_document" "cluster_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${local.name}-cluster"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
    "arn:aws:iam::aws:policy/AmazonEKSComputePolicy",
    "arn:aws:iam::aws:policy/AmazonEKSBlockStoragePolicy",
    "arn:aws:iam::aws:policy/AmazonEKSLoadBalancingPolicy",
    "arn:aws:iam::aws:policy/AmazonEKSNetworkingPolicy",
  ])

  role       = aws_iam_role.cluster.name
  policy_arn = each.value
}

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${local.name}-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodeMinimalPolicy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPullOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = each.value
}

resource "aws_eks_cluster" "outboxer" {
  name     = local.name
  role_arn = aws_iam_role.cluster.arn

  # Auto Mode: compute, storage, and load balancing managed by EKS; the
  # bootstrap addons are replaced by the Auto Mode equivalents.
  bootstrap_self_managed_addons = false

  compute_config {
    enabled       = true
    node_pools    = ["general-purpose"]
    node_role_arn = aws_iam_role.node.arn
  }

  kubernetes_network_config {
    elastic_load_balancing {
      enabled = true
    }
  }

  storage_config {
    block_storage {
      enabled = true
    }
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    subnet_ids              = aws_subnet.public[*].id
    endpoint_public_access  = true
    endpoint_private_access = true
  }

  depends_on = [aws_iam_role_policy_attachment.cluster]
}

# --- Pod Identity: the outboxer pods' AWS permissions --------------------------------

data "aws_iam_policy_document" "workload_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "workload" {
  name               = "${local.name}-workload"
  assume_role_policy = data.aws_iam_policy_document.workload_assume.json
}

resource "aws_iam_role_policy" "workload_sqs" {
  name = "sqs-send"
  role = aws_iam_role.workload.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage", "sqs:GetQueueAttributes"]
      Resource = [aws_sqs_queue.events.arn, aws_sqs_queue.events_fifo.arn]
    }]
  })
}

resource "aws_eks_pod_identity_association" "outboxer" {
  cluster_name    = aws_eks_cluster.outboxer.name
  namespace       = "outboxer"
  service_account = "outboxer"
  role_arn        = aws_iam_role.workload.arn
}
