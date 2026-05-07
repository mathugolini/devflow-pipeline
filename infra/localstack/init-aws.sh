#!/usr/bin/env bash
# Provisions SQS queues (with DLQs) and DynamoDB tables on LocalStack.
# Idempotent: re-running is safe — existing resources are skipped.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localstack:4566}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="000000000000"

aws_sqs() { aws --endpoint-url="$ENDPOINT" --region "$REGION" sqs "$@"; }
aws_ddb() { aws --endpoint-url="$ENDPOINT" --region "$REGION" dynamodb "$@"; }

echo "[init-aws] waiting for LocalStack at $ENDPOINT..."
for i in $(seq 1 60); do
  if aws_sqs list-queues >/dev/null 2>&1; then
    echo "[init-aws] LocalStack is ready"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "[init-aws] LocalStack did not become ready in time" >&2
    exit 1
  fi
  sleep 1
done

create_queue() {
  local name="$1"
  local attrs="${2:-}"
  if aws_sqs get-queue-url --queue-name "$name" >/dev/null 2>&1; then
    echo "[init-aws] queue $name already exists"
    return
  fi
  if [ -n "$attrs" ]; then
    aws_sqs create-queue --queue-name "$name" --attributes "$attrs" >/dev/null
  else
    aws_sqs create-queue --queue-name "$name" >/dev/null
  fi
  echo "[init-aws] created queue $name"
}

dlq_arn() {
  echo "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:$1"
}

# DLQs first — main queues reference them.
create_queue "raw-events-dlq"
create_queue "processed-events-dlq"

create_queue "raw-events" "$(cat <<EOF
{
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$(dlq_arn raw-events-dlq)\",\"maxReceiveCount\":\"3\"}",
  "VisibilityTimeout": "30"
}
EOF
)"

create_queue "processed-events" "$(cat <<EOF
{
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$(dlq_arn processed-events-dlq)\",\"maxReceiveCount\":\"3\"}",
  "VisibilityTimeout": "30"
}
EOF
)"

create_table() {
  local name="$1"
  local key="$2"
  if aws_ddb describe-table --table-name "$name" >/dev/null 2>&1; then
    echo "[init-aws] table $name already exists"
    return
  fi
  aws_ddb create-table \
    --table-name "$name" \
    --attribute-definitions AttributeName="$key",AttributeType=S \
    --key-schema AttributeName="$key",KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
  echo "[init-aws] created table $name"
}

# events has both a primary HASH on event_id and a GSI on developer_id
# (developer_id-index) so the API can Query by developer without Scan.
create_events_table() {
  if aws_ddb describe-table --table-name "events" >/dev/null 2>&1; then
    echo "[init-aws] table events already exists"
    return
  fi
  aws_ddb create-table \
    --table-name "events" \
    --attribute-definitions \
        AttributeName=event_id,AttributeType=S \
        AttributeName=developer_id,AttributeType=S \
    --key-schema AttributeName=event_id,KeyType=HASH \
    --global-secondary-indexes '[{
      "IndexName": "developer_id-index",
      "KeySchema": [{"AttributeName":"developer_id","KeyType":"HASH"}],
      "Projection": {"ProjectionType":"ALL"}
    }]' \
    --billing-mode PAY_PER_REQUEST >/dev/null
  echo "[init-aws] created table events with developer_id-index GSI"
}

enable_ttl() {
  local name="$1"
  local attr="$2"
  local current
  current=$(aws_ddb describe-time-to-live --table-name "$name" \
      --query 'TimeToLiveDescription.TimeToLiveStatus' --output text 2>/dev/null || echo "")
  if [ "$current" = "ENABLED" ] || [ "$current" = "ENABLING" ]; then
    echo "[init-aws] TTL on $name already $current"
    return
  fi
  if ! aws_ddb update-time-to-live --table-name "$name" \
      --time-to-live-specification "Enabled=true,AttributeName=$attr" >/dev/null 2>&1; then
    echo "[init-aws] TTL update on $name returned non-zero (probably already on); continuing"
    return
  fi
  echo "[init-aws] TTL enabled on $name (attr=$attr)"
}

create_events_table
create_table "developer_summary" "developer_id"
enable_ttl "events" "expires_at"

echo "[init-aws] done"
