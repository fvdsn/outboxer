#!/usr/bin/env bash
# Deploys the ephemeral Outboxer-on-EKS test stack:
#   1. Terraform: EKS Auto Mode cluster, RDS, SQS, ECR, Pod Identity IAM.
#   2. Build and push the image.
#   3. kubectl: render and apply the manifests in k8s/, run the init Job to
#      completion, then roll out the relay Deployment.
set -euo pipefail

REGION="${OUTBOXER_AWS_REGION:-eu-central-1}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INFRA="$ROOT/deploy/aws-eks/infra"
K8S="$ROOT/deploy/aws-eks/k8s"
TAG="${IMAGE_TAG:-$(git -C "$ROOT" rev-parse --short HEAD)}"

# A dedicated kubeconfig keeps this stack out of the operator's personal
# kubectl contexts. The test recipes export the same path.
export KUBECONFIG="$ROOT/deploy/aws-eks/.kubeconfig"

command -v kubectl > /dev/null || { echo "kubectl is required"; exit 1; }
command -v aws > /dev/null || { echo "aws CLI is required"; exit 1; }

echo "=== infra: bootstrap (image repository)"
terraform -chdir="$INFRA" init -input=false > /dev/null
terraform -chdir="$INFRA" apply -input=false -auto-approve \
  -var "region=$REGION" \
  -target=aws_ecr_repository.outboxer

echo "=== image: build and push"
REPO="$(terraform -chdir="$INFRA" output -raw ecr_repository)"
IMAGE="${REPO}:${TAG}"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}" > /dev/null
docker build --platform linux/amd64 -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

echo "=== infra: full apply (EKS cluster and RDS take ~10 min, in parallel)"
terraform -chdir="$INFRA" apply -input=false -auto-approve \
  -var "region=$REGION"

echo "=== kubectl: connect to the cluster"
CLUSTER="$(terraform -chdir="$INFRA" output -raw cluster)"
aws eks update-kubeconfig --name "$CLUSTER" --region "$REGION" --kubeconfig "$KUBECONFIG"

echo "=== kubectl: render and apply manifests"
QUEUE_URL="$(terraform -chdir="$INFRA" output -raw queue_url)"
DB_HOST="$(terraform -chdir="$INFRA" output -raw db_host)"
DB_PASSWORD="$(terraform -chdir="$INFRA" output -raw db_password)"
RENDERED="$(mktemp -d)"
for manifest in "$K8S"/*.yaml; do
  sed -e "s|\${REGION}|$REGION|g" \
      -e "s|\${QUEUE_URL}|$QUEUE_URL|g" \
      -e "s|\${DB_HOST}|$DB_HOST|g" \
      -e "s|\${IMAGE}|$IMAGE|g" \
      "$manifest" > "$RENDERED/$(basename "$manifest")"
done

kubectl apply -f "$RENDERED/namespace.yaml"
kubectl apply -f "$RENDERED/serviceaccount.yaml"
kubectl apply -f "$RENDERED/configmap.yaml"
# The password never touches a manifest file: the Secret is created directly.
kubectl -n outboxer create secret generic outboxer-db \
  --from-literal=password="$DB_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== schema: run init job to completion (first pod also provisions a node)"
kubectl -n outboxer delete job outboxer-init --ignore-not-found
kubectl apply -f "$RENDERED/init-job.yaml"
kubectl -n outboxer wait --for=condition=complete job/outboxer-init --timeout=900s

echo "=== relay: roll out"
kubectl apply -f "$RENDERED/deployment.yaml"
kubectl -n outboxer rollout status deployment/outboxer --timeout=900s

terraform -chdir="$INFRA" output -json > "$ROOT/test/cloud/awseks/tfoutputs.json"
rm -rf "$RENDERED"
echo "=== DEPLOY COMPLETE"
