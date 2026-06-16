# Outboxer on ECS Fargate

Sample Terraform for running Outboxer as an AWS ECS Fargate service.

Edit the `locals` block in `main.tf`, then apply the stack from this directory.
The example assumes the ECS cluster, VPC/subnets/security groups, PostgreSQL
database, SQS queue, and Secrets Manager secret already exist. It creates:

- a CloudWatch log group
- an ECS task execution role for image pulls, logs, and secret reads
- an ECS task role with SQS publish permissions
- a Fargate task definition
- an ECS service

ECS Fargate is the closest AWS equivalent to the Cloud Run deployment style: a
managed container service with ECS keeping one or more tasks running. Keep
`desired_count = 1` unless you have tested your outbox table, ordering
requirements, and queue destinations with multiple workers.

Use private subnets with egress through NAT, or add VPC endpoints for ECR,
CloudWatch Logs, Secrets Manager, STS, and SQS as needed. If the image stays in
GHCR, the task also needs outbound access to GHCR unless you mirror the image to
ECR.
