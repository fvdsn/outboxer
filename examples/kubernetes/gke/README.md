# Outboxer on GKE

Sample Kubernetes manifests for running Outboxer on Google Kubernetes Engine.

Edit `outboxer.yaml`, then apply it:

```sh
kubectl apply -f outboxer.yaml
```

This example focuses on the Kubernetes workload only:

- a Namespace
- a Kubernetes ServiceAccount annotated for GKE Workload Identity
- a Deployment with resource requests, limits, and health probes

Before applying it, create the Google service account, grant it Pub/Sub publish
permissions, and bind it to the Kubernetes service account with Workload
Identity. Also create the `outboxer-db` Kubernetes Secret, or replace the secret
reference with your preferred secret-sync mechanism.

Keep `replicas: 1` unless you have tested your outbox table, ordering
requirements, and queue destinations with multiple workers.
