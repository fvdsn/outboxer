#!/usr/bin/env bash
# Deploys the ephemeral Outboxer-on-GKE test stack:
#   1. Terraform: cluster, Cloud SQL, Pub/Sub, Artifact Registry, IAM.
#   2. Build and push the image.
#   3. kubectl: render and apply the manifests in k8s/, run the init Job to
#      completion, then roll out the relay Deployment.
set -euo pipefail

: "${OUTBOXER_GCP_PROJECT:?set OUTBOXER_GCP_PROJECT (e.g. in .env)}"
PROJECT="$OUTBOXER_GCP_PROJECT"
REGION="${OUTBOXER_GCP_REGION:-europe-west1}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INFRA="$ROOT/deploy/gcp-gke/infra"
K8S="$ROOT/deploy/gcp-gke/k8s"
TAG="${IMAGE_TAG:-$(git -C "$ROOT" rev-parse --short HEAD)}"

# A dedicated kubeconfig keeps this stack out of the operator's personal
# kubectl contexts. The test recipes export the same path.
export KUBECONFIG="$ROOT/deploy/gcp-gke/.kubeconfig"

command -v kubectl > /dev/null || { echo "kubectl is required"; exit 1; }
gcloud container clusters --help > /dev/null 2>&1 || { echo "gcloud is required"; exit 1; }

echo "=== infra: bootstrap (APIs + image repository)"
terraform -chdir="$INFRA" init -input=false > /dev/null
terraform -chdir="$INFRA" apply -input=false -auto-approve \
  -var "project_id=$PROJECT" -var "region=$REGION" \
  -target=google_project_service.apis -target=google_artifact_registry_repository.outboxer

echo "=== image: build and push"
REPO="$(terraform -chdir="$INFRA" output -raw artifact_repository)"
IMAGE="$REPO/outboxer:$TAG"
gcloud auth application-default print-access-token \
  | docker login -u oauth2accesstoken --password-stdin "$REGION-docker.pkg.dev" > /dev/null
docker build --platform linux/amd64 -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

echo "=== infra: full apply (cluster and Cloud SQL take ~10 min, in parallel)"
terraform -chdir="$INFRA" apply -input=false -auto-approve \
  -var "project_id=$PROJECT" -var "region=$REGION"

echo "=== kubectl: connect to the cluster"
gcloud container clusters get-credentials outboxer-gke \
  --region "$REGION" --project "$PROJECT"

echo "=== kubectl: render and apply manifests"
CONN="$(terraform -chdir="$INFRA" output -raw cloudsql_connection_name)"
TOPIC="$(terraform -chdir="$INFRA" output -raw topic)"
DB_PASSWORD="$(terraform -chdir="$INFRA" output -raw db_password)"
RENDERED="$(mktemp -d)"
for manifest in "$K8S"/*.yaml; do
  sed -e "s|\${PROJECT_ID}|$PROJECT|g" \
      -e "s|\${REGION}|$REGION|g" \
      -e "s|\${TOPIC}|$TOPIC|g" \
      -e "s|\${CLOUDSQL_CONNECTION_NAME}|$CONN|g" \
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

echo "=== schema: run init job to completion"
kubectl -n outboxer delete job outboxer-init --ignore-not-found
kubectl apply -f "$RENDERED/init-job.yaml"
kubectl -n outboxer wait --for=condition=complete job/outboxer-init --timeout=600s

echo "=== relay: roll out"
kubectl apply -f "$RENDERED/deployment.yaml"
kubectl -n outboxer rollout status deployment/outboxer --timeout=600s

terraform -chdir="$INFRA" output -json > "$ROOT/test/cloud/gcpgke/tfoutputs.json"
rm -rf "$RENDERED"
echo "=== DEPLOY COMPLETE"
