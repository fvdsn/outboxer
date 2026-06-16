# Outboxer on GKE

Sample Terraform for running Outboxer on Google Kubernetes Engine with Workload
Identity.

Edit the `locals` block in `main.tf`, configure the `kubernetes` provider for
your cluster, then apply the stack from this directory. The example assumes the
GKE cluster, PostgreSQL database, Pub/Sub topics, and Kubernetes Secret already
exist. It creates:

- a Google service account for Outboxer
- `roles/pubsub.publisher`
- a Kubernetes service account annotated for Workload Identity
- a Kubernetes Deployment

Keep `replicas = 1` unless you have tested your outbox table, ordering
requirements, and queue destinations with multiple workers.
