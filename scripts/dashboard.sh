#!/usr/bin/env bash
# Real-time dashboard: queue depths + DDB row counts + recent events.
# Run with: bash scripts/dashboard.sh
# Or with refresh: watch -n 2 -c bash scripts/dashboard.sh
set -uo pipefail

# Run with --loop (or LOOP=1) to auto-refresh every 2s, no external `watch` needed.
LOOP="${LOOP:-0}"
INTERVAL="${INTERVAL:-2}"
[ "${1:-}" = "--loop" ] && LOOP=1
[ "${1:-}" = "--once" ] && LOOP=0

ENDPOINT="${ENDPOINT:-http://localhost:4566}"
API="${API:-http://localhost:8080}"

if command -v aws >/dev/null 2>&1; then
  aws_cmd() { aws --endpoint-url="$ENDPOINT" --region us-east-1 "$@" 2>/dev/null; }
else
  aws_cmd() {
    docker run --rm --network host \
      -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
      amazon/aws-cli:2.17.0 --endpoint-url="$ENDPOINT" --region us-east-1 "$@" 2>/dev/null
  }
fi

if [ -t 1 ]; then
  C_BLD=$'\033[1m'; C_DIM=$'\033[2m'; C_GRN=$'\033[32m'
  C_YEL=$'\033[33m'; C_RED=$'\033[31m'; C_BLU=$'\033[34m'; C_RST=$'\033[0m'
else
  C_BLD=""; C_DIM=""; C_GRN=""; C_YEL=""; C_RED=""; C_BLU=""; C_RST=""
fi

qdepth() {
  aws_cmd sqs get-queue-attributes \
    --queue-url "$ENDPOINT/000000000000/$1" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo "?"
}

table_count() {
  aws_cmd dynamodb scan --table-name "$1" --select COUNT \
    --query 'Count' --output text 2>/dev/null || echo "?"
}

color_q() {
  local n="$1"
  if [ "$n" = "0" ]; then echo "${C_GRN}$n${C_RST}"
  elif [ "$n" = "?" ]; then echo "${C_RED}?${C_RST}"
  else echo "${C_YEL}$n${C_RST}"; fi
}

color_dlq() {
  local n="$1"
  if [ "$n" = "0" ]; then echo "${C_GRN}$n${C_RST}"
  elif [ "$n" = "?" ]; then echo "${C_RED}?${C_RST}"
  else echo "${C_RED}${C_BLD}$n${C_RST}"; fi
}

render() {
clear
echo "${C_BLD}${C_BLU}┌─ DevFlow Pipeline Dashboard ─ $(date +%T) ───────────────────┐${C_RST}"

# Health
health=$(curl -s -m 2 "$API/health" 2>/dev/null || echo '{}')
status=$(echo "$health" | jq -r '.status // "down"' 2>/dev/null)
ddb=$(echo "$health" | jq -r '.checks.dynamodb // "?"' 2>/dev/null)
sqs=$(echo "$health" | jq -r '.checks.sqs // "?"' 2>/dev/null)
case "$status" in
  ok) status_c="${C_GRN}ok${C_RST}" ;;
  *)  status_c="${C_RED}${status}${C_RST}" ;;
esac
echo "${C_BLD}  health${C_RST}        $status_c   ${C_DIM}(ddb=$ddb sqs=$sqs)${C_RST}"
echo

# Queues
raw=$(qdepth raw-events)
proc=$(qdepth processed-events)
draw=$(qdepth raw-events-dlq)
dproc=$(qdepth processed-events-dlq)

echo "${C_BLD}  filas SQS${C_RST}"
printf "    raw-events            %s\n" "$(color_q "$raw")"
printf "    processed-events      %s\n" "$(color_q "$proc")"
printf "    raw-events-dlq        %s\n" "$(color_dlq "$draw")"
printf "    processed-events-dlq  %s\n" "$(color_dlq "$dproc")"
echo

# Tables
ev=$(table_count events)
sm=$(table_count developer_summary)
echo "${C_BLD}  DynamoDB${C_RST}"
printf "    events                ${C_BLU}%s${C_RST} rows\n" "$ev"
printf "    developer_summary     ${C_BLU}%s${C_RST} devs\n" "$sm"
echo

# Top developers
echo "${C_BLD}  top developers${C_RST}"
aws_cmd dynamodb scan --table-name developer_summary \
  --projection-expression 'developer_id, events_processed, total_commits, total_pull_requests, last_activity' \
  --output json 2>/dev/null \
  | jq -r '.Items[] | "    \(.developer_id.S | ascii_downcase | .[0:24] | . + (" " * (24 - length)))  ev=\(.events_processed.N // "0")  c=\(.total_commits.N // "0")  pr=\(.total_pull_requests.N // "0")  last=\(.last_activity.S // "—")"' 2>/dev/null \
  | sort -k3 -t= -nr | head -8

echo
if [ "$LOOP" = "1" ]; then
  echo "${C_DIM}  Ctrl+C pra sair  ·  refresh ${INTERVAL}s${C_RST}"
else
  echo "${C_DIM}  refresh: bash scripts/dashboard.sh --loop  (ou: make watch)${C_RST}"
fi
echo "${C_BLD}${C_BLU}└──────────────────────────────────────────────────────────────┘${C_RST}"
}

if [ "$LOOP" = "1" ]; then
  trap 'tput cnorm 2>/dev/null; exit 0' INT TERM
  tput civis 2>/dev/null
  while true; do render; sleep "$INTERVAL"; done
else
  render
fi
