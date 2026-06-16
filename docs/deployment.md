# Deployment

Outboxer is distributed as a container image:

```text
ghcr.io/fvdsn/outboxer:v0.1.0
```

The image is built for `linux/amd64` and `linux/arm64`. Pin an explicit version
in production instead of `latest`.

This repository includes sample deployment examples for four common targets:

| Target | Example |
| --- | --- |
| GCP Cloud Run | [`examples/terraform/gcp-cloud-run`](../examples/terraform/gcp-cloud-run) |
| GCP GKE | [`examples/kubernetes/gke`](../examples/kubernetes/gke) |
| AWS ECS Fargate | [`examples/terraform/aws-ecs-fargate`](../examples/terraform/aws-ecs-fargate) |
| AWS EKS | [`examples/kubernetes/eks`](../examples/kubernetes/eks) |

The examples are intentionally small copy-and-edit references, not
registry-published modules. The Cloud Run and ECS examples use Terraform because
those targets are cloud resources. The GKE and EKS examples use Kubernetes YAML
and leave the cloud-side IAM setup to the reader.

All examples assume that PostgreSQL, Pub/Sub topics, SQS queues, VPC networking,
and Kubernetes clusters already exist.

## Common Checklist

- Run Outboxer under a supervisor that restarts it on exit.
- Start with one replica or task. Increase only after testing ordering,
  database locking, and provider quotas for your workload.
- Use cloud-native identity: Cloud Run service accounts, GKE Workload Identity,
  ECS task roles, or EKS IRSA.
- Store database credentials in Secret Manager, Secrets Manager, or Kubernetes
  Secrets. Avoid plaintext secrets in Terraform state when possible.
- Ensure the runtime can reach PostgreSQL and queue APIs from its network.
- Set `LOG_FORMAT=json` for managed logs.
- Keep `PUBLISH_TIMEOUT_MS`, `PUBLISH_RESULT_GRACE_MS`, and
  `WATCHDOG_INTERVAL_MS` consistent with the sizing guidance in
  [`processing.md`](processing.md).
- Consumers must be idempotent; Outboxer provides at-least-once delivery.

## GCP Cloud Run

Use Cloud Run when you want a simple managed container runtime on GCP. The
example creates a Cloud Run v2 service with:

- a dedicated Google service account
- `roles/pubsub.publisher`
- Secret Manager access for configured secrets
- CPU allocated outside request handling with `cpu_idle = false`
- `min_instances = 1` by default so the worker keeps polling

Cloud Run is request-oriented by default, so the important production detail is
to run it like a worker: keep at least one instance warm and keep CPU available
between requests. Configure VPC access in the calling stack when PostgreSQL is
private.

See [`examples/terraform/gcp-cloud-run`](../examples/terraform/gcp-cloud-run).

## GCP GKE

Use GKE when Outboxer should run next to other Kubernetes workloads. The
Kubernetes YAML creates:

- a Kubernetes service account annotated for Workload Identity
- a Deployment with resource requests, limits, and a liveness probe

Create the Google service account, Pub/Sub publisher IAM binding, and Workload
Identity binding separately.

The Kubernetes Secret containing `PG_PASSWORD` is intentionally left to the
calling stack. That lets each installation choose External Secrets, Secret
Manager sync, sealed secrets, or plain Kubernetes Secrets.

See [`examples/kubernetes/gke`](../examples/kubernetes/gke).

## AWS ECS Fargate

Use ECS Fargate when you want a serverless AWS container runtime without
managing Kubernetes. The example creates:

- a CloudWatch log group
- an ECS task execution role for image pulls, logs, and secret reads
- an ECS task role for SQS publishing
- a Fargate task definition
- an ECS service that keeps the task running

Run it in private subnets with NAT egress, or provide VPC endpoints for the AWS
APIs the task needs. If the container image stays in GHCR, the task needs
outbound internet access unless you mirror the image into ECR.

See [`examples/terraform/aws-ecs-fargate`](../examples/terraform/aws-ecs-fargate).

## AWS EKS

Use EKS when Outboxer should run in AWS-managed Kubernetes. The Kubernetes YAML
creates:

- a Kubernetes service account annotated with that IAM role
- a Deployment with resource requests, limits, and a liveness probe

Create the IAM role, IRSA trust policy, and SQS permissions separately.

As with GKE, Kubernetes Secrets are left to the calling stack.

See [`examples/kubernetes/eks`](../examples/kubernetes/eks).

## Cross-Cloud Publishing

Native deployments are simplest: Pub/Sub from GCP and SQS from AWS. Cross-cloud
publishing is supported, but the identity setup is more involved. See
[`auth.md`](auth.md) for workload identity federation details.
