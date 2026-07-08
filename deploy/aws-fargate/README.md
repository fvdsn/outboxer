# Outboxer on AWS ECS Fargate

A complete, ephemeral Outboxer deployment on Fargate: RDS PostgreSQL, a
standard and a FIFO SQS queue, and the relay as a single always-on task.
Deployed and measured by the cloud integration tests
(`test/cloud/awsfargate`); doubles as the reference AWS serverless setup.

## Design

- **Minimal dedicated VPC, no NAT gateway.** Two public subnets and an
  internet gateway; the task gets a public IP and security groups do the
  gating. The NAT gateway is the classic standing-cost trap of ephemeral AWS
  stacks (~$35/month plus per-GB) and is deliberately absent.
- **RDS is publicly addressable but firewalled to exactly two callers**: the
  task's security group and the operator's current IP (resolved at apply
  time, for the test harness). This is the test-stack equivalent of the GCP
  stacks' IAM-gated connectors. RDS forces TLS (`rds.force_ssl`), so the
  relay connects with `PG_SSL=true`; certificate verification is off here —
  a production deployment should pin the RDS CA bundle and use private
  subnets.
- **Native task-role auth**: the relay publishes to SQS with the ECS task
  role via the SDK's default credential chain — no keys anywhere. The
  execution role (image pull, password secret, logs) is separate, following
  the ECS two-role model.
- **Init before relay**: `outboxer init --apply` runs as a one-off ECS task
  and must succeed before the service is created (`deploy_relay` flips in the
  second apply) — the relay fails fast without a schema.
- **Single-active relay**: `desired_count = 1` with
  `deployment_maximum_percent = 100` / `minimum_healthy_percent = 0` gives
  replace-style deployments, the Fargate equivalent of Kubernetes' Recreate.
- **Two queues**: SQS splits what Pub/Sub unifies — ordered delivery lives on
  the `.fifo` queue (`messageGroupId` per event), unordered on the standard
  queue.

## Usage

Prerequisites: `terraform`, `docker`, and the `aws` CLI authenticated against
the intended **test** account (set `AWS_PROFILE` if needed). Optionally set
`OUTBOXER_AWS_REGION` in the repo's `.env` (default `eu-central-1`).

```sh
just cloud-aws-fargate-up      # ~10 min, dominated by RDS creation
just cloud-aws-fargate-test    # functional scenarios
just cloud-aws-fargate-perf    # performance run, writes test/cloud/results/*.json
just cloud-aws-fargate-latency # idle-state end-to-end latency percentiles
just cloud-aws-fargate-down    # destroy everything
```

Sizing is realistic by default (~$0.65/hour while up): a 2 vCPU / 4 GB task
and a `db.m7g.xlarge` (4 vCPU / 16 GB) RDS instance. Terraform state is local
and gitignored; every resource is tagged `outboxer-test = true`.

Note: the security groups admit the IP you had at `apply` time. If your IP
changes mid-session, re-run the apply (the up recipe is idempotent) to
refresh the allowlist.
