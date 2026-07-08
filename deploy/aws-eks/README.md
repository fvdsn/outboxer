# Outboxer on EKS (Auto Mode)

A complete, ephemeral Outboxer deployment on Amazon EKS, deployed and
measured by the cloud integration tests (`test/cloud/awseks`). The Kubernetes
manifests in [`k8s/`](k8s/) are the reference artifact to copy for a real
deployment; the Terraform in [`infra/`](infra/) provisions only what
Kubernetes cannot describe (the cluster itself, RDS, SQS, image repository,
IAM).

## Layout — why both Terraform and YAML

Kubernetes YAML describes what runs *inside* a cluster. The cluster, the
database, the queues, and the permissions are AWS resources, so the realistic
split — and the one production teams use — is infrastructure as Terraform,
application as manifests applied with `kubectl`:

```
infra/   cluster (EKS Auto Mode), RDS PostgreSQL, SQS, ECR, Pod Identity IAM
k8s/     namespace, service account, config, init Job, relay Deployment
up.sh    the whole deploy sequence, in order, readable
down.sh  terraform destroy (deleting the cluster deletes the k8s objects)
```

## The Kubernetes patterns used, and why

- **EKS Auto Mode**: AWS manages nodes, scaling, and the core addons — the
  EKS analog of GKE Autopilot, and the least-ceremony way to run a cluster.
  The first pod scheduled triggers node provisioning, so a fresh deploy pays
  a minute or two of Pending on the init Job.
- **EKS Pod Identity**: pods authenticate to SQS as their Kubernetes service
  account. An association in `infra/` maps `ns/outboxer/sa/outboxer` to an
  IAM role whose trust principal is `pods.eks.amazonaws.com` — the modern
  successor to IRSA: no OIDC provider, no service-account annotations, no
  key files, nothing to rotate.
- **No database sidecar**: unlike Cloud SQL, RDS is plain TCP. The relay
  connects directly to the instance endpoint with TLS (`PG_SSL=true`);
  certificate verification is off for this test stack — production should
  mount the RDS CA bundle and set `PG_SSL_ROOT_CERT`. Network access is the
  RDS security group admitting the VPC (pods) and the operator's apply-time
  IP (the test harness).
- **`replicas: 1` + `strategy: Recreate`**: Outboxer is a single-active
  worker. A rolling update would briefly run two relays against the same
  table — safe (they serialize on row locks; delivery is at-least-once) but
  Recreate keeps the model honest.
- **The init Job runs before the Deployment** (`up.sh` waits for completion):
  the relay fails fast, by design, when the schema is missing.
- **Probes**: the startup probe on `/healthz` doubles as a schema and
  database connectivity gate, because Outboxer binds its health server only
  after the startup checks pass. The liveness probe uses Outboxer's honest
  batch-staleness check — provider outages never trip it, so the kubelet
  does not restart the relay for problems a restart cannot fix. See
  [Observability](../../docs/observability.md).
- **The database password never touches a manifest**: `up.sh` creates the
  Secret directly from the Terraform output.

## Usage

Prerequisites: `terraform`, `kubectl`, `docker`, and the `aws` CLI
(authenticated; `aws eks update-kubeconfig` also provides kubectl's
credentials, so no extra plugin is needed). Set `AWS_PROFILE` (and
optionally `OUTBOXER_AWS_REGION`, default `eu-central-1`) in the repo's
`.env`.

```sh
just cloud-aws-eks-up      # ~12 min: EKS cluster and RDS create in parallel
just cloud-aws-eks-test    # functional scenarios (via kubectl port-forward)
just cloud-aws-eks-perf    # performance run, writes test/cloud/results/*.json
just cloud-aws-eks-down    # destroy everything
```

Sizing is realistic by default (the stack exists for performance measurement
and is paid by the hour): 2 vCPU / 1 Gi for the relay pod (Auto Mode picks a
node to fit), a `db.m7g.xlarge` RDS instance — roughly $0.90/hour while up.
Terraform state is local and gitignored; every resource carries the
`outboxer-test` tag, so `just cloud-aws-orphans` finds an orphaned stack even
without state.
