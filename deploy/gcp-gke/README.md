# Outboxer on GKE (Autopilot)

A complete, ephemeral Outboxer deployment on Google Kubernetes Engine,
deployed and measured by the cloud integration tests (`test/cloud/gcpgke`).
The Kubernetes manifests in [`k8s/`](k8s/) are the reference artifact to copy
for a real deployment; the Terraform in [`infra/`](infra/) provisions only
what Kubernetes cannot describe (the cluster itself, Cloud SQL, Pub/Sub,
image repository, IAM).

## Layout — why both Terraform and YAML

Kubernetes YAML describes what runs *inside* a cluster. The cluster, the
database, the topic, and the permissions are Google Cloud resources, so the
realistic split — and the one production teams use — is infrastructure as
Terraform, application as manifests applied with `kubectl`:

```
infra/   cluster (Autopilot), Cloud SQL, Pub/Sub, Artifact Registry, IAM
k8s/     namespace, service account, config, init Job, relay Deployment
up.sh    the whole deploy sequence, in order, readable
down.sh  terraform destroy (deleting the cluster deletes the k8s objects)
```

## The Kubernetes patterns used, and why

- **Workload Identity Federation**: pods authenticate to Pub/Sub and Cloud
  SQL as their Kubernetes service account
  (`ns/outboxer/sa/outboxer`) via IAM bindings on the workload identity
  principal — no Google service account, no key files, nothing to rotate.
- **cloud-sql-proxy as a native sidecar** (an init container with
  `restartPolicy: Always`, Kubernetes ≥ 1.29): the canonical way to reach
  Cloud SQL from a pod. The proxy is ready before the main container starts
  and is torn down with it — which is also what lets the init *Job* actually
  complete. The relay just connects to `127.0.0.1:5432`.
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

Prerequisites: `terraform`, `kubectl`, `docker`, `gcloud` (authenticated,
with application-default credentials and the `gke-gcloud-auth-plugin`
component), and `cloud-sql-proxy` for the test harness. Set
`OUTBOXER_GCP_PROJECT` (and optionally `OUTBOXER_GCP_REGION`) in the repo's
`.env`.

```sh
just cloud-gcp-gke-up      # ~12 min: cluster and Cloud SQL create in parallel
just cloud-gcp-gke-test    # functional scenarios (via kubectl port-forward)
just cloud-gcp-gke-perf    # performance run, writes test/cloud/results/*.json
just cloud-gcp-gke-down    # destroy everything
```

Sizing is realistic by default (the stack exists for performance measurement
and is paid by the hour): 2 vCPU / 1 Gi for the relay pod, a 4 vCPU / 16 GB
Cloud SQL instance — roughly $0.65/hour while up. Terraform state is local
and gitignored; every resource carries the `outboxer-test` label so an
orphaned stack can be found even without state.
