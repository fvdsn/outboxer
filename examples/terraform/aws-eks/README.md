# Outboxer on EKS

Sample Terraform for running Outboxer on Amazon EKS with IAM Roles for Service
Accounts (IRSA).

Edit the `locals` block in `main.tf`, configure the `kubernetes` provider for
your cluster, then apply the stack from this directory. The example assumes the
EKS cluster, PostgreSQL database, SQS queue, and Kubernetes Secret already
exist. It creates:

- an IAM role trusted by the Kubernetes service account
- SQS publish permissions
- a Kubernetes service account annotated for IRSA
- a Kubernetes Deployment

Keep `replicas = 1` unless you have tested your outbox table, ordering
requirements, and queue destinations with multiple workers.
