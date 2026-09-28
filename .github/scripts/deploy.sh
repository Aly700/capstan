#!/usr/bin/env bash
set -euo pipefail

: "${AWS_ACCOUNT_ID:?Set AWS_ACCOUNT_ID}"
: "${AWS_REGION:?Set AWS_REGION}"
: "${COMMIT_SHA:?Set COMMIT_SHA}"
: "${BUDGET_EMAIL:?Set BUDGET_EMAIL}"
[[ "$AWS_ACCOUNT_ID" =~ ^[0-9]{12}$ ]]
[[ "$COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$AWS_REGION" == us-east-1 ]]

# The pinned credentials action does not support allowed-account-ids. Check the
# assumed identity before any ECR, Docker, or CloudFormation operation.
deployment_account=$(aws sts get-caller-identity --query Account --output text)
if [[ "$deployment_account" != "$AWS_ACCOUNT_ID" ]]; then
  echo "Refusing deployment: assumed AWS account does not match AWS_ACCOUNT_ID" >&2
  exit 1
fi

deployment_tmp=$(mktemp -d)
trap 'rm -rf "$deployment_tmp"' EXIT
registry="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
image="$registry/capstan-server:$COMMIT_SHA"

# Immutable tags make retrying a dispatch safe. Only ImageNotFound permits a push;
# permission or network failures must not be mistaken for a missing image.
if aws ecr describe-images --repository-name capstan-server --image-ids "imageTag=$COMMIT_SHA" > /dev/null 2> "$deployment_tmp/ecr-error"; then
  echo "Reusing image $COMMIT_SHA"
elif grep -q 'ImageNotFoundException' "$deployment_tmp/ecr-error"; then
  aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$registry"
  docker buildx build --platform linux/arm64 --tag "$image" --push .
else
  cat "$deployment_tmp/ecr-error" >&2
  exit 1
fi

cd infra
npx cdk deploy --all --require-approval never --no-lookups \
  --role-arn "arn:aws:iam::$AWS_ACCOUNT_ID:role/capstan-cloudformation" \
  -c workloadOnly=true -c "imageTag=$COMMIT_SHA" -c "budgetEmail=$BUDGET_EMAIL" \
  -c "gateUrl=${GATE_URL:-}" --outputs-file "$deployment_tmp/outputs.json"

api_url=$(node --input-type=module - "$deployment_tmp/outputs.json" <<'JS'
import { readFileSync } from 'node:fs';
const url = JSON.parse(readFileSync(process.argv[2], 'utf8')).CapstanService.ApiUrl;
if (!/^https:\/\/[a-z0-9]+\.execute-api\.us-east-1\.amazonaws\.com$/.test(url)) throw new Error('Unexpected API URL');
console.log(url);
JS
)
for path in healthz readyz; do
  curl --fail --silent --show-error --retry 8 --retry-all-errors --retry-delay 5 \
    --retry-max-time 90 --connect-timeout 5 --max-time 30 "$api_url/$path"
done
echo "Smoke passed: $api_url"
