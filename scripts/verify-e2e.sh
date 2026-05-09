#!/usr/bin/env bash
# Runs the full pipeline (raw-events -> Processor -> processed-events
# -> Aggregator -> DynamoDB -> API) and asserts each stage.
#
# Exits 0 only when every assertion passes. Polls instead of sleeping
# so it's robust on slow machines; fails fast with a clear message
# when something is off.
#
# Prereqs: `make up` running. The script uses the project's AWS CLI
# fallback (local binary, else docker amazon/aws-cli on the compose
# network), exactly as scripts/seed.sh does.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"
API_BASE="${API_BASE:-http://localhost:8080}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

if command -v aws >/dev/null 2>&1; then
  AWS_BIN=("aws" "--endpoint-url=$ENDPOINT" "--region" "$REGION")
else
  AWS_BIN=(docker run --rm --network devflow-pipeline_devflow \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    amazon/aws-cli:2.17.0 --endpoint-url=http://localstack:4566 --region "$REGION")
fi

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }

fail() { red "[verify] FAIL: $*" >&2; exit 1; }
pass() { green "[verify] PASS: $*"; }
step() { blue  "[verify] -- $* --"; }

queue_url() {
  "${AWS_BIN[@]}" sqs get-queue-url --queue-name "$1" \
    --query QueueUrl --output text | tr -d '\r'
}

queue_attr() {
  local url="$1" attr="$2"
  "${AWS_BIN[@]}" sqs get-queue-attributes \
    --queue-url "$url" --attribute-names "$attr" \
    --query "Attributes.$attr" --output text | tr -d '\r'
}

table_count() {
  "${AWS_BIN[@]}" dynamodb scan --table-name "$1" \
    --select COUNT --query Count --output text | tr -d '\r'
}

uuid() { python3 -c 'import uuid;print(uuid.uuid4())' 2>/dev/null || uuidgen | tr '[:upper:]' '[:lower:]'; }
ts()   { date -u +%Y-%m-%dT%H:%M:%SZ; }

# ---------------------------------------------------------------------
# 0. Pre-flight: queues + tables exist, API responds
# ---------------------------------------------------------------------
step "pre-flight"

RAW_URL=$(queue_url raw-events)
DLQ_URL=$(queue_url raw-events-dlq)
PROCESSED_URL=$(queue_url processed-events)
[ -n "$RAW_URL" ] && [ -n "$DLQ_URL" ] && [ -n "$PROCESSED_URL" ] \
  || fail "missing one of the SQS queues — run 'make up' first"

table_count events       >/dev/null || fail "events table missing"
table_count developer_summary >/dev/null || fail "developer_summary table missing"

if ! curl -fsS "$API_BASE/health" >/dev/null; then
  fail "API not reachable at $API_BASE/health — is the aggregator up?"
fi
pass "infra + API reachable"

# ---------------------------------------------------------------------
# 1. Capture baseline (so the script is safe to re-run)
# ---------------------------------------------------------------------
step "baseline"
BASE_EVENTS=$(table_count events)
BASE_DLQ=$(queue_attr "$DLQ_URL" ApproximateNumberOfMessages)
echo "  events.count=$BASE_EVENTS  raw-events-dlq.depth=$BASE_DLQ"

# ---------------------------------------------------------------------
# 2. Seed
# ---------------------------------------------------------------------
step "seed"
bash "$(dirname "$0")/seed.sh"

# ---------------------------------------------------------------------
# 3. Drain: wait until raw-events is empty AND
#    events.count grew by at least 19 (10+5+3+1 dedup).
#    Poll every 2s up to TIMEOUT_SECONDS — no fixed sleep.
# ---------------------------------------------------------------------
step "drain (timeout=${TIMEOUT_SECONDS}s)"
deadline=$((SECONDS + TIMEOUT_SECONDS))
while [ $SECONDS -lt $deadline ]; do
  in_flight=$(queue_attr "$RAW_URL" ApproximateNumberOfMessages)
  not_visible=$(queue_attr "$RAW_URL" ApproximateNumberOfMessagesNotVisible)
  events_now=$(table_count events)
  delta=$((events_now - BASE_EVENTS))
  if [ "$in_flight" = "0" ] && [ "$not_visible" = "0" ] && [ "$delta" -ge 19 ]; then
    pass "drained — events delta=$delta, raw-events idle"
    break
  fi
  sleep 2
done
if [ "$delta" -lt 19 ]; then
  fail "drain timeout — events delta=$delta (want >=19), raw in_flight=$in_flight not_visible=$not_visible"
fi

# ---------------------------------------------------------------------
# 4. Persistence assertions
# ---------------------------------------------------------------------
step "persistence"
[ "$delta" -eq 19 ] || echo "  note: events delta=$delta (expected 19 — extras can come from prior runs; not fatal)"

DEV1_SUMMARY=$(curl -fsS "$API_BASE/metrics/dev-1/summary")
DEV2_SUMMARY=$(curl -fsS "$API_BASE/metrics/dev-2/summary")

# Pull a field via grep (no jq dependency).
field() {
  local body="$1" name="$2"
  echo "$body" | tr ',{}' '\n' | grep -E "\"$name\"" \
    | head -1 | sed -E "s/.*\"$name\"[[:space:]]*:[[:space:]]*//; s/[\" ]//g"
}

D1_COMMITS=$(field "$DEV1_SUMMARY" total_commits)
D1_RT_COUNT=$(field "$DEV1_SUMMARY" review_time_count)
D1_AVG=$(field "$DEV1_SUMMARY" avg_review_time_minutes)
D2_PRS=$(field "$DEV2_SUMMARY" total_pull_requests)

# 10 valid commits + 1 deduped commit = 11; review_time count = 3 (15+30+45 -> avg 30)
[ "$D1_COMMITS" -ge 11 ] || fail "dev-1.total_commits=$D1_COMMITS (want >=11)"
[ "$D1_RT_COUNT" -ge 3 ] || fail "dev-1.review_time_count=$D1_RT_COUNT (want >=3)"
[ "${D1_AVG%.*}" = "30" ] || fail "dev-1.avg_review_time_minutes=$D1_AVG (want ~30)"
[ "$D2_PRS" -ge 5 ] || fail "dev-2.total_pull_requests=$D2_PRS (want >=5)"
pass "summaries match expected counts"

EVENTS_DEV1=$(curl -fsS "$API_BASE/metrics/dev-1")
echo "$EVENTS_DEV1" | grep -q '"event_id"' || fail "GET /metrics/dev-1 returned no events"
pass "GET /metrics/dev-1 returns events"

# ---------------------------------------------------------------------
# 5. DLQ — invalid events should land here after maxReceiveCount=3.
# Each rejected message must hit the visibility timeout 3 times before
# SQS redrives it, so the wait must cover ~3 × VisibilityTimeout. The
# queue is provisioned with VisibilityTimeout=30s → minimum ~90s.
# ---------------------------------------------------------------------
step "dlq (waits ~3x VisibilityTimeout for redrive)"
dlq_deadline=$((SECONDS + 120))
while [ $SECONDS -lt $dlq_deadline ]; do
  dlq_now=$(queue_attr "$DLQ_URL" ApproximateNumberOfMessages)
  delta_dlq=$((dlq_now - BASE_DLQ))
  if [ "$delta_dlq" -ge 2 ]; then
    pass "raw-events-dlq grew by $delta_dlq (>=2 invalid)"
    break
  fi
  sleep 2
done
[ "$delta_dlq" -ge 2 ] || fail "raw-events-dlq delta=$delta_dlq (want >=2)"

# ---------------------------------------------------------------------
# 6. Idempotency — send the SAME event_id 3x, assert events grew by 1
# ---------------------------------------------------------------------
step "idempotency"
DUP_ID=$(uuid)
DUP_BODY="{\"event_id\":\"$DUP_ID\",\"developer_id\":\"verify-dup\",\"metric_type\":\"commits\",\"value\":1,\"repository\":\"verify/idem\",\"timestamp\":\"$(ts)\"}"
PRE=$(table_count events)
for _ in 1 2 3; do
  "${AWS_BIN[@]}" sqs send-message --queue-url "$RAW_URL" --message-body "$DUP_BODY" >/dev/null
done

idem_deadline=$((SECONDS + 30))
while [ $SECONDS -lt $idem_deadline ]; do
  POST=$(table_count events)
  if [ "$POST" -ge $((PRE + 1)) ]; then
    break
  fi
  sleep 2
done
GROWTH=$((POST - PRE))
[ "$GROWTH" -eq 1 ] || fail "idempotency broken: 3 sends of same event_id grew events by $GROWTH (want 1)"

# also assert summary did not double-count
DUP_SUMMARY=$(curl -fsS "$API_BASE/metrics/verify-dup/summary")
DUP_COMMITS=$(field "$DUP_SUMMARY" total_commits)
DUP_PROCESSED=$(field "$DUP_SUMMARY" events_processed)
[ "$DUP_COMMITS" = "1" ] || fail "verify-dup.total_commits=$DUP_COMMITS (want 1)"
[ "$DUP_PROCESSED" = "1" ] || fail "verify-dup.events_processed=$DUP_PROCESSED (want 1)"
pass "idempotency holds (1 row, 1 commit, events_processed=1)"

green "[verify] all assertions passed"
