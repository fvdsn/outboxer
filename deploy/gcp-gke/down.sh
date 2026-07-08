#!/usr/bin/env bash
# Destroys the GKE test stack. Deleting the cluster takes the Kubernetes
# objects with it, so Terraform destroy is the whole teardown.
set -euo pipefail

: "${OUTBOXER_GCP_PROJECT:?set OUTBOXER_GCP_PROJECT (e.g. in .env)}"
REGION="${OUTBOXER_GCP_REGION:-europe-west1}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INFRA="$ROOT/deploy/gcp-gke/infra"

terraform -chdir="$INFRA" destroy -input=false -auto-approve \
  -var "project_id=$OUTBOXER_GCP_PROJECT" -var "region=$REGION"
rm -f "$ROOT/test/cloud/gcpgke/tfoutputs.json" "$ROOT/deploy/gcp-gke/.kubeconfig"
