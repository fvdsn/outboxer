# Outboxer on Cloud Run

Sample Terraform for running Outboxer as a continuously running Cloud Run
worker.

Edit the `locals` block in `main.tf`, then apply the stack from this directory.
The example assumes PostgreSQL and Pub/Sub topics already exist. It creates:

- a Google service account for Outboxer
- `roles/pubsub.publisher`
- Secret Manager access for the configured database password secret
- a Cloud Run v2 service using `ghcr.io/fvdsn/outboxer:v0.1.0`

Cloud Run services normally optimize for request/response workloads. For
Outboxer, keep `min_instance_count = 1` and `cpu_idle = false` so the processor
can run without inbound requests.

For private databases, add the VPC connector / network configuration that fits
your project. This sample deliberately does not create Cloud SQL, VPCs, topics,
or database users.
