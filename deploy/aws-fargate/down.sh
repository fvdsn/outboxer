#!/usr/bin/env bash
# Destroys the Fargate test stack.
set -euo pipefail

REGION="${OUTBOXER_AWS_REGION:-eu-central-1}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STACK="$ROOT/deploy/aws-fargate"

terraform -chdir="$STACK" destroy -input=false -auto-approve \
  -var "region=$REGION" -var "image=unused" -var deploy_relay=true
rm -f "$ROOT/test/cloud/awsfargate/tfoutputs.json"
