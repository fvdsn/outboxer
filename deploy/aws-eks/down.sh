#!/usr/bin/env bash
# Destroys the EKS test stack. Deleting the cluster takes the Kubernetes
# objects with it, so Terraform destroy is the whole teardown. Auto Mode
# drains and removes its own nodes as part of cluster deletion.
set -euo pipefail

REGION="${OUTBOXER_AWS_REGION:-eu-central-1}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INFRA="$ROOT/deploy/aws-eks/infra"

terraform -chdir="$INFRA" destroy -input=false -auto-approve \
  -var "region=$REGION"
rm -f "$ROOT/test/cloud/awseks/tfoutputs.json" "$ROOT/deploy/aws-eks/.kubeconfig"
