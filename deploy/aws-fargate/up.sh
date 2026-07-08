#!/usr/bin/env bash
# Deploys the ephemeral Outboxer-on-Fargate test stack:
#   1. Terraform bootstrap: the ECR repository.
#   2. Build and push the image.
#   3. Terraform full apply with deploy_relay=false (everything but the relay).
#   4. Run `outboxer init --apply` as a one-off ECS task, to completion.
#   5. Terraform apply with deploy_relay=true and wait for the service.
set -euo pipefail

REGION="${OUTBOXER_AWS_REGION:-eu-central-1}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STACK="$ROOT/deploy/aws-fargate"
TAG="${IMAGE_TAG:-$(git -C "$ROOT" rev-parse --short HEAD)}"

command -v aws > /dev/null || { echo "aws cli is required"; exit 1; }
aws sts get-caller-identity --query Account --output text > /dev/null || { echo "aws credentials not configured"; exit 1; }

echo "=== infra: bootstrap (ECR)"
terraform -chdir="$STACK" init -input=false > /dev/null
terraform -chdir="$STACK" apply -input=false -auto-approve \
  -var "region=$REGION" -target=aws_ecr_repository.outboxer

echo "=== image: build and push"
REPO="$(terraform -chdir="$STACK" output -raw ecr_repository)"
IMAGE="$REPO:$TAG"
aws ecr get-login-password --region "$REGION" \
  | docker login -u AWS --password-stdin "${REPO%%/*}" > /dev/null
docker build --platform linux/amd64 -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

echo "=== infra: full apply without the relay (RDS takes ~5-10 min)"
terraform -chdir="$STACK" apply -input=false -auto-approve \
  -var "region=$REGION" -var "image=$IMAGE" -var deploy_relay=false

echo "=== schema: run init task to completion"
CLUSTER="$(terraform -chdir="$STACK" output -raw cluster)"
TASKDEF="$(terraform -chdir="$STACK" output -raw task_definition)"
SUBNETS="$(terraform -chdir="$STACK" output -raw subnet_ids)"
SG="$(terraform -chdir="$STACK" output -raw task_security_group)"
TASK_ARN="$(aws ecs run-task --region "$REGION" --cluster "$CLUSTER" \
  --launch-type FARGATE --task-definition "$TASKDEF" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"outboxer","command":["init","--apply"]}]}' \
  --query 'tasks[0].taskArn' --output text)"
aws ecs wait tasks-stopped --region "$REGION" --cluster "$CLUSTER" --tasks "$TASK_ARN"
EXIT_CODE="$(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$TASK_ARN" \
  --query 'tasks[0].containers[0].exitCode' --output text)"
if [ "$EXIT_CODE" != "0" ]; then
  echo "init task failed with exit code $EXIT_CODE; logs:"
  aws logs tail "/ecs/outboxer" --region "$REGION" --since 10m | tail -20
  exit 1
fi

echo "=== relay: deploy the service"
terraform -chdir="$STACK" apply -input=false -auto-approve \
  -var "region=$REGION" -var "image=$IMAGE" -var deploy_relay=true
aws ecs wait services-stable --region "$REGION" --cluster "$CLUSTER" --services outboxer

terraform -chdir="$STACK" output -json > "$ROOT/test/cloud/awsfargate/tfoutputs.json"
echo "=== DEPLOY COMPLETE"
