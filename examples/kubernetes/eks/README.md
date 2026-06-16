# Outboxer on EKS

Sample Kubernetes manifests for running Outboxer on Amazon EKS.

Edit `outboxer.yaml`, then apply it:

```sh
kubectl apply -f outboxer.yaml
```

This example focuses on the Kubernetes workload only:

- a Namespace
- a Kubernetes ServiceAccount annotated for IAM Roles for Service Accounts
  (IRSA)
- a Deployment with resource requests, limits, and health probes

Before applying it, create the IAM role, trust the cluster OIDC provider for the
Kubernetes service account, and grant the role SQS publish permissions. Also
create the `outboxer-db` Kubernetes Secret, or replace the secret reference with
your preferred secret-sync mechanism.

Keep `replicas: 1` unless you have tested your outbox table, ordering
requirements, and queue destinations with multiple workers.
