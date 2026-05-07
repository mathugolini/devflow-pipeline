#!/usr/bin/env bash
# Seeds the raw-events queue with a mix of valid + invalid + duplicate
# events for end-to-end smoke testing of the pipeline.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

if command -v aws >/dev/null 2>&1; then
  AWS_BIN=("aws" "--endpoint-url=$ENDPOINT" "--region" "$REGION")
else
  AWS_BIN=(docker run --rm --network devflow-pipeline_devflow \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    amazon/aws-cli:2.17.0 --endpoint-url=http://localstack:4566 --region "$REGION")
fi

uuid() { python3 -c 'import uuid;print(uuid.uuid4())' 2>/dev/null || uuidgen | tr '[:upper:]' '[:lower:]'; }
ts()   { date -u +%Y-%m-%dT%H:%M:%SZ; }

QURL=$("${AWS_BIN[@]}" sqs get-queue-url --queue-name raw-events --query QueueUrl --output text | tr -d '\r')

send() {
  local body="$1"
  "${AWS_BIN[@]}" sqs send-message --queue-url "$QURL" --message-body "$body" >/dev/null
}

DEV1="dev-1"; DEV2="dev-2"

# 10 valid commit events for dev-1
for i in $(seq 1 10); do
  send "{\"event_id\":\"$(uuid)\",\"developer_id\":\"$DEV1\",\"metric_type\":\"commits\",\"value\":1,\"repository\":\"org/api\",\"timestamp\":\"$(ts)\"}"
done

# 5 valid PR events for dev-2
for i in $(seq 1 5); do
  send "{\"event_id\":\"$(uuid)\",\"developer_id\":\"$DEV2\",\"metric_type\":\"pull_requests\",\"value\":1,\"repository\":\"org/api\",\"timestamp\":\"$(ts)\"}"
done

# 3 valid review_time events for dev-1
for v in 15 30 45; do
  send "{\"event_id\":\"$(uuid)\",\"developer_id\":\"$DEV1\",\"metric_type\":\"review_time_minutes\",\"value\":$v,\"repository\":\"org/api\",\"timestamp\":\"$(ts)\"}"
done

# 1 duplicate (same event_id sent twice) — exercises idempotency
DUP_ID=$(uuid)
DUP_BODY="{\"event_id\":\"$DUP_ID\",\"developer_id\":\"$DEV1\",\"metric_type\":\"commits\",\"value\":1,\"repository\":\"org/api\",\"timestamp\":\"$(ts)\"}"
send "$DUP_BODY"
send "$DUP_BODY"

# 2 invalid events — go to DLQ
send '{"event_id":"not-a-uuid","developer_id":"","metric_type":"deploys","value":-1,"timestamp":"2099-01-01T00:00:00Z"}'
send "{\"event_id\":\"$(uuid)\",\"developer_id\":\"dev-x\",\"metric_type\":\"commits\",\"value\":-99,\"repository\":\"org/api\",\"timestamp\":\"$(ts)\"}"

echo "[seed] sent ~21 events to raw-events"
