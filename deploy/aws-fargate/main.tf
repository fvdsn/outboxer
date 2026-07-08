# Ephemeral, realistically sized Outboxer deployment on ECS Fargate, deployed
# and measured by the cloud integration tests (test/cloud/awsfargate) and
# doubling as the reference AWS serverless setup. Created per test run,
# destroyed afterwards.
#
# Design choices, and where they differ from the GCP stacks:
# - AWS has no "no-VPC" mode: the stack creates a minimal dedicated VPC with
#   two public subnets and an internet gateway — and deliberately no NAT
#   gateway (the single standing-cost trap of ephemeral AWS stacks). The task
#   gets a public IP; security groups do the gating.
# - RDS is publicly addressable but firewalled to exactly two callers: the
#   task's security group and the operator's current IP (resolved at apply
#   time). This is the pragmatic equivalent of GCP's IAM-gated connectors for
#   a test stack; a production deployment would use private subnets.
# - RDS PostgreSQL 15+ forces TLS (rds.force_ssl=1), so the relay connects
#   with PG_SSL=true. Certificate verification is off for the test stack —
#   trusting the RDS CA bundle is a production concern documented in the
#   README.
# - The relay authenticates to SQS with the native ECS task role; the AWS SDK
#   default credential chain picks it up with no configuration.
# - Everything is tagged outboxer-test=true for orphan sweeps.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
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
      app           = "outboxer"
      outboxer-test = "true"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

# The operator's current IP, allowed through the security groups so the test
# harness can reach the database and the relay's metrics port.
data "http" "operator_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  name          = "outboxer"
  operator_cidr = "${chomp(data.http.operator_ip.response_body)}/32"
}

# --- Networking ---------------------------------------------------------------

resource "aws_vpc" "outboxer" {
  cidr_block           = "10.90.0.0/16"
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

resource "aws_security_group" "task" {
  name   = "${local.name}-task"
  vpc_id = aws_vpc.outboxer.id

  ingress {
    description = "Relay health/metrics from the operator"
    from_port   = 8080
    to_port     = 8080
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

resource "aws_security_group" "rds" {
  name   = "${local.name}-rds"
  vpc_id = aws_vpc.outboxer.id

  ingress {
    description     = "Postgres from the relay"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.task.id]
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
  name = local.name

  # Ephemeral: allow destroy with images still inside.
  force_delete = true
}

# --- Database ---------------------------------------------------------------------

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "db_password" {
  name = "${local.name}-db-password"

  # Ephemeral: skip the soft-delete recovery window so repeated up/down
  # cycles can reuse the name.
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = random_password.db.result
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

  # Ephemeral test infrastructure.
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
  name       = "${local.name}-events.fifo"
  fifo_queue = true
  # Outboxer supplies per-message deduplication ids.
  content_based_deduplication = false
}

# --- ECS ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "outboxer" {
  name = local.name
}

resource "aws_cloudwatch_log_group" "outboxer" {
  name              = "/ecs/${local.name}"
  retention_in_days = 1
}

data "aws_iam_policy_document" "task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# The task role: what the relay itself may do — publish to its two queues.
resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json
}

resource "aws_iam_role_policy" "task_sqs" {
  name = "sqs-send"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage", "sqs:GetQueueAttributes"]
      Resource = [aws_sqs_queue.events.arn, aws_sqs_queue.events_fifo.arn]
    }]
  })
}

# The execution role: what ECS needs to start the container — pull the image,
# read the password secret, write logs.
resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secret" {
  name = "read-db-password"
  role = aws_iam_role.execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [aws_secretsmanager_secret.db_password.arn]
    }]
  })
}

resource "aws_ecs_task_definition" "outboxer" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "outboxer"
    image     = var.image
    essential = true

    portMappings = [{ containerPort = 8080, protocol = "tcp" }]

    environment = [
      { name = "LOG_FORMAT", value = "json" },
      { name = "SQS_ENABLED", value = "true" },
      { name = "AWS_REGION", value = var.region },
      { name = "DEFAULT_SQS_QUEUE_URL", value = aws_sqs_queue.events.url },
      { name = "DLQ_TABLE", value = "outboxer_dead_letters" },
      { name = "COLLECT_BATCH_TARGET", value = tostring(var.collect_batch_target) },
      { name = "SQS_SEND_CONCURRENCY", value = tostring(var.sqs_send_concurrency) },
      { name = "PG_HOST", value = aws_db_instance.outboxer.address },
      { name = "PG_USER", value = "outboxer" },
      { name = "PG_DATABASE", value = "outboxer" },
      # RDS forces TLS; certificate verification would require distributing
      # the RDS CA bundle — a production concern, skipped for the test stack.
      { name = "PG_SSL", value = "true" },
      { name = "PG_SSL_REJECT_UNAUTHORIZED", value = "false" },
      { name = "HEALTH_PORT", value = "8080" },
    ]

    secrets = [{
      name      = "PG_PASSWORD"
      valueFrom = aws_secretsmanager_secret.db_password.arn
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.outboxer.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "outboxer"
      }
    }
  }])
}

# The relay: a single always-on task. Created only after the up recipe has run
# the schema init task (deploy_relay flips to true), because the relay fails
# fast without a schema. minimum_healthy_percent = 0 with maximum_percent =
# 100 gives replace-style deployments — the single-active-relay equivalent of
# Kubernetes' Recreate.
resource "aws_ecs_service" "outboxer" {
  count = var.deploy_relay ? 1 : 0

  name            = local.name
  cluster         = aws_ecs_cluster.outboxer.id
  task_definition = aws_ecs_task_definition.outboxer.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = true
  }
}
