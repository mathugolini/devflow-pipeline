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

create_table "events" "event_id"
create_table "developer_summary" "developer_id"

echo "[init-aws] done"
