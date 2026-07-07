# Outboxer on GCP Cloud Run

A complete, ephemeral Outboxer deployment on Cloud Run: Cloud SQL (Postgres),
a Pub/Sub topic with an ordered subscription, a Cloud Run Job that provisions
the schema with `outboxer init --apply`, and the relay as a single always-on
Cloud Run instance. This stack is what the cloud integration tests
(`test/cloud/gcpcloudrun`) deploy and measure, so it is continuously proven —
it also serves as the reference setup to copy for a real deployment.

## Design

- **No VPC.** Cloud SQL has a public IP with zero authorized networks, so it
  is reachable only through IAM-gated connectors: the relay uses Cloud Run's
  built-in `/cloudsql` unix-socket mount (`PG_HOST=/cloudsql/<connection
  name>`; Outboxer's pgx driver treats a leading `/` as a socket directory),
  and the test harness uses `cloud-sql-proxy`. This removes the VPC, peering,
  and serverless connector entirely.
- **Regional Pub/Sub endpoint.** Publishing with ordering keys requires it;
  the stack sets `PUBSUB_API_ENDPOINT` accordingly.
- **Single always-on instance.** `min = max = 1`, CPU allocated outside
  requests: Outboxer is a worker, not a request handler. Cloud Run's injected
  `PORT` doubles as Outboxer's health/metrics server, which satisfies the
  startup probe.
- **Ephemeral by construction.** No deletion protection anywhere, a random
  suffix on the Cloud SQL instance name (deleted names are unusable for ~a
  week), and every resource labeled `outboxer-test = true` so an orphaned
  stack can be found (`just cloud-gcp-orphans`) even if the local Terraform
  state is lost.
- **Realistic sizing by default** (~$0.55/hour): the stack exists for
  performance measurement, and it is paid by the hour, not by the month.

## Usage

Prerequisites: `terraform`, `gcloud` (authenticated, with application-default
credentials), `docker`, and `cloud-sql-proxy` for the test harness. Set
`OUTBOXER_GCP_PROJECT` (and optionally `OUTBOXER_GCP_REGION`) in the repo's
`.env`.

```sh
just cloud-gcp-cloudrun-up      # ~12 min, dominated by Cloud SQL creation
just cloud-gcp-cloudrun-test    # functional scenarios
just cloud-gcp-cloudrun-perf    # performance run, writes test/cloud/results/*.json
just cloud-gcp-cloudrun-down    # destroy everything
```

The up recipe applies in two phases (repository first, then everything) so it
can push the locally built image to Artifact Registry in between, then runs
the init job to provision the schema.

Terraform state is local to this directory and gitignored: this stack is
operated by one person at a time and exists for under an hour. If the state is
lost while a stack is up, `just cloud-gcp-orphans` lists what survived.
