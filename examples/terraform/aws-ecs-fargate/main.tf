terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

locals {
  aws_region         = "eu-west-1"
  name               = "outboxer"
  image              = "ghcr.io/fvdsn/outboxer:v0.1.0"
  cluster_arn        = "arn:aws:ecs:eu-west-1:123456789012:cluster/app"
  subnet_ids         = ["subnet-0123456789abcdef0", "subnet-abcdef0123456789a"]
  security_group_ids = ["sg-0123456789abcdef0"]
  queue_url          = "https://sqs.eu-west-1.amazonaws.com/123456789012/events"
  queue_arn          = "arn:aws:sqs:eu-west-1:123456789012:events"

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
      value_from = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:outboxer-db-password"
      read_arn   = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:outboxer-db-password-*"
    }
  }

  container_environment = [
    for key, value in local.env : {
      name  = key
      value = value
    }
  ]

  container_secrets = [
    for key, value in local.secret_env : {
      name      = key
      valueFrom = value.value_from
    }
  ]
}

provider "aws" {
  region = local.aws_region
}

resource "aws_cloudwatch_log_group" "outboxer" {
  name              = "/ecs/${local.name}"
  retention_in_days = 30
  tags              = local.tags
}

data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [for value in values(local.secret_env) : value.read_arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "${local.name}-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "task_sqs" {
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

resource "aws_iam_role_policy" "task_sqs" {
  name   = "${local.name}-sqs"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_sqs.json
}

resource "aws_ecs_task_definition" "outboxer" {
  family                   = local.name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "outboxer"
      image     = local.image
      essential = true

      portMappings = [
        {
          containerPort = 8080
          hostPort      = 8080
          protocol      = "tcp"
        }
      ]

      environment = local.container_environment
      secrets     = local.container_secrets

      healthCheck = {
        command     = ["CMD-SHELL", "wget -q -O - http://127.0.0.1:8080/ || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 10
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.outboxer.name
          awslogs-region        = local.aws_region
          awslogs-stream-prefix = "outboxer"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "outboxer" {
  name            = local.name
  cluster         = local.cluster_arn
  task_definition = aws_ecs_task_definition.outboxer.arn
  desired_count   = 1
  launch_type     = "FARGATE"
  tags            = local.tags

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = local.subnet_ids
    security_groups  = local.security_group_ids
    assign_public_ip = false
  }

  depends_on = [
    aws_iam_role_policy.task_sqs,
    aws_iam_role_policy.execution_secrets,
    aws_iam_role_policy_attachment.execution,
  ]
}
