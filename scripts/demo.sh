#!/usr/bin/env bash
# Narrated end-to-end demo for an evaluator running through the project
# for the first time. Each stage announces what is being tested and
# *why*, executes, and asserts. Exit 0 only when every stage passes.
#
# Usage:
#   bash scripts/demo.sh           # standard demo (~2 min)
#   bash scripts/demo.sh --full    # standard demo + 12 stress scenarios
#
# Side effects on the host:
#   - opens browser tabs for Jaeger UI and the API docs at the end
#     (skipped silently when no `open`/`xdg-open` is available)
set -uo pipefail

FULL=0
[ "${1:-}" = "--full" ] && FULL=1

# ── colors / glyphs ──────────────────────────────────────────────────
if [ -t 1 ]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'
  C_BLU=$'\033[34m'; C_DIM=$'\033[2m';  C_BLD=$'\033[1m'; C_RST=$'\033[0m'
else
  C_RED=""; C_GRN=""; C_YEL=""; C_BLU=""; C_DIM=""; C_BLD=""; C_RST=""
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
API_BASE="${API_BASE:-http://localhost:8080}"
JAEGER_BASE="${JAEGER_BASE:-http://localhost:16686}"
NETWORK="${COMPOSE_NETWORK:-devflow-pipeline_devflow}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

if command -v aws >/dev/null 2>&1; then
  AWS_BIN=("aws" "--endpoint-url=$ENDPOINT" "--region" "us-east-1")
else
  AWS_BIN=(docker run --rm --network "$NETWORK" \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    amazon/aws-cli:2.17.0 --endpoint-url=http://devflow-localstack:4566 --region us-east-1)
fi

# ── helpers ──────────────────────────────────────────────────────────
banner() {
  local n="$1" total="$2" title="$3"
  printf '\n%s▶ ETAPA %s/%s — %s%s\n' "$C_BLU$C_BLD" "$n" "$total" "$title" "$C_RST"
}
why()   { printf '  %sPor quê: %s%s\n' "$C_DIM" "$1" "$C_RST"; }
note()  { printf '  %s%s%s\n'           "$C_DIM" "$1" "$C_RST"; }
pass()  { printf '  %s✓%s %s\n'         "$C_GRN" "$C_RST" "$1"; }
fail()  { printf '  %s✗%s %s\n' "$C_RED" "$C_RST" "$1" >&2; exit 1; }
sep()   { printf '%s%s%s\n' "$C_BLU" '═══════════════════════════════════════════════════════════════════' "$C_RST"; }

q_url()  { "${AWS_BIN[@]}" sqs get-queue-url --queue-name "$1" --query QueueUrl --output text 2>/dev/null | tr -d '\r'; }
q_attr() { "${AWS_BIN[@]}" sqs get-queue-attributes --queue-url "$1" --attribute-names "$2" --query "Attributes.$2" --output text 2>/dev/null | tr -d '\r'; }
t_count(){ "${AWS_BIN[@]}" dynamodb scan --table-name "$1" --select COUNT --query Count --output text 2>/dev/null | tr -d '\r'; }

# ── header ───────────────────────────────────────────────────────────
clear 2>/dev/null || true
sep
printf '%s  DEVFLOW PIPELINE — DEMO INTERATIVA%s\n' "$C_BLD" "$C_RST"
note  '  Cada etapa cita o requisito do case e prova com assert.'
sep

TOTAL=6
[ "$FULL" -eq 1 ] && TOTAL=7

# ─────────────────────────────────────────────────────────────────────
banner 1 "$TOTAL" "Subindo o stack via docker compose"
why   "case: \"tudo deve rodar com um único comando: docker-compose up\""
note  "Aguardando containers ficarem prontos..."
( cd "$ROOT_DIR" && docker compose up -d 2>&1 ) | tail -3
deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl -fsS "$API_BASE/health" >/dev/null 2>&1 \
     && curl -fsS "$JAEGER_BASE/api/services" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$API_BASE/health"           >/dev/null || fail "aggregator API não respondeu em 60s"
curl -fsS "$JAEGER_BASE/api/services"  >/dev/null || fail "Jaeger UI não respondeu em 60s"
pass "5 containers up: localstack, aws-init(exited 0), processor, aggregator, jaeger"
pass "/health 200 (SQS + DynamoDB ok), Jaeger /api/services responde"

# ─────────────────────────────────────────────────────────────────────
banner 2 "$TOTAL" "Captura de baseline"
why   "demo é safe-to-rerun — mede deltas, não absolutos"
RAW_URL=$(q_url raw-events)
DLQ_URL=$(q_url raw-events-dlq)
BASE_EVENTS=$(t_count events)
BASE_DLQ=$(q_attr "$DLQ_URL" ApproximateNumberOfMessages)
pass "events.count=$BASE_EVENTS  raw-events-dlq.depth=$BASE_DLQ"

# ─────────────────────────────────────────────────────────────────────
banner 3 "$TOTAL" "Populando dados de teste (seed)"
why   "21 eventos mistos exercitam todas as 5 regras de validação + idempotência"
( cd "$ROOT_DIR" && bash scripts/seed.sh ) 2>&1 | tail -1
pass "18 válidos (commits, PRs, review_time) + 1 duplicata + 2 inválidos"

# ─────────────────────────────────────────────────────────────────────
banner 4 "$TOTAL" "Drain + persistência"
why   "prova que o fluxo Processor → SQS → Aggregator → DynamoDB funciona"
note  "Polling a cada 2s (sem sleep fixo) — timeout 60s..."
deadline=$(( $(date +%s) + 60 ))
delta=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  in_flight=$(q_attr "$RAW_URL" ApproximateNumberOfMessages)
  not_visible=$(q_attr "$RAW_URL" ApproximateNumberOfMessagesNotVisible)
  events_now=$(t_count events)
  delta=$(( events_now - BASE_EVENTS ))
  if [ "$in_flight" = "0" ] && [ "$not_visible" = "0" ] && [ "$delta" -ge 19 ]; then break; fi
  sleep 2
done
[ "$delta" -ge 19 ] || fail "drain timeout: events delta=$delta (esperado >=19)"
pass "events table cresceu $delta linhas (esperado >=19)"

DEV1_SUMMARY=$(curl -fsS "$API_BASE/metrics/dev-1/summary")
field() { echo "$1" | tr ',{}' '\n' | grep -E "\"$2\"" | head -1 | sed -E "s/.*\"$2\"[[:space:]]*:[[:space:]]*//; s/[\" ]//g"; }
D1_COMMITS=$(field "$DEV1_SUMMARY" total_commits)
D1_AVG=$(field "$DEV1_SUMMARY" avg_review_time_minutes)
[ "$D1_COMMITS" -ge 11 ] || fail "GET /metrics/dev-1/summary total_commits=$D1_COMMITS (esperado >=11)"
pass "GET /metrics/dev-1/summary: $D1_COMMITS commits, avg ${D1_AVG} min review"
EVENTS_DEV1=$(curl -fsS "$API_BASE/metrics/dev-1")
echo "$EVENTS_DEV1" | grep -q '"event_id"' || fail "GET /metrics/dev-1 sem eventos"
pass "GET /metrics/dev-1 retorna eventos (Query no GSI developer_id-index)"

note  "Aguardando DLQ redrive (3× VisibilityTimeout=30s)..."
deadline=$(( $(date +%s) + 120 ))
delta_dlq=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  dlq_now=$(q_attr "$DLQ_URL" ApproximateNumberOfMessages)
  delta_dlq=$(( dlq_now - BASE_DLQ ))
  [ "$delta_dlq" -ge 2 ] && break
  sleep 3
done
[ "$delta_dlq" -ge 2 ] || fail "raw-events-dlq delta=$delta_dlq (esperado >=2)"
pass "raw-events-dlq: $delta_dlq mensagens (validação rejeitou os inválidos)"

# Idempotência ao vivo
DUP_ID=$(uuidgen 2>/dev/null | tr 'A-Z' 'a-z' || python3 -c 'import uuid;print(uuid.uuid4())')
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
DUP_BODY="{\"event_id\":\"$DUP_ID\",\"developer_id\":\"demo-dup\",\"metric_type\":\"commits\",\"value\":1,\"repository\":\"d/d\",\"timestamp\":\"$TS\"}"
PRE=$(t_count events)
for _ in 1 2 3; do "${AWS_BIN[@]}" sqs send-message --queue-url "$RAW_URL" --message-body "$DUP_BODY" >/dev/null; done
deadline=$(( $(date +%s) + 30 ))
POST=$PRE
while [ "$(date +%s)" -lt "$deadline" ]; do
  POST=$(t_count events)
  [ "$POST" -ge $((PRE + 1)) ] && break
  sleep 2
done
GROWTH=$(( POST - PRE ))
[ "$GROWTH" -eq 1 ] || fail "Idempotência quebrada: 3× mesmo event_id → cresceu $GROWTH (esperado 1)"
pass "Idempotência: 3× mesmo event_id → 1 linha (ConditionalCheckFailedException working)"

# ─────────────────────────────────────────────────────────────────────
banner 5 "$TOTAL" "Tracing distribuído (OpenTelemetry)"
why   "prova que o trace_id atravessa Processor e Aggregator via SQS MessageAttributes"
sleep 3
TRACES=$(curl -fsS "$JAEGER_BASE/api/traces?service=devflow-aggregator&limit=30")
# JSON arrives on a single line — count by occurrences (-o), not by line.
N_TRACES=$(printf '%s' "$TRACES" | grep -oE '"traceID":' | wc -l | tr -d ' ')
[ "$N_TRACES" -ge 5 ] || fail "Jaeger só registrou $N_TRACES traces para o aggregator (esperado >=5)"
pass "Jaeger registrou $N_TRACES traces no aggregator"
TID=$(printf '%s' "$TRACES" | grep -oE '"traceID":"[a-f0-9]+"' | head -1 | cut -d'"' -f4)
[ -n "$TID" ] || fail "não consegui extrair um trace_id do Jaeger"
N_SERVICES=$(curl -fsS "$JAEGER_BASE/api/traces/$TID" | grep -oE '"serviceName":"[^"]+"' | sort -u | wc -l | tr -d ' ')
[ "$N_SERVICES" -ge 2 ] || fail "trace $TID cobre $N_SERVICES serviço (esperado 2: processor + aggregator)"
pass "trace $TID atravessa $N_SERVICES serviços (W3C traceparent via SQS)"

# ─────────────────────────────────────────────────────────────────────
banner 6 "$TOTAL" "Documentação da API + UIs"
why   "OpenAPI 3.1 servido pelo próprio aggregator (sem geração via anotações)"
curl -fsS "$API_BASE/openapi.yaml" >/dev/null || fail "GET /openapi.yaml falhou"
pass "GET /openapi.yaml retorna spec embedded (//go:embed)"
curl -fsS "$API_BASE/docs" | grep -q 'spec-url="/openapi.yaml"' || fail "/docs não aponta ReDoc para o spec"
pass "GET /docs renderiza ReDoc"

if [ "$FULL" -eq 1 ]; then
  banner 7 "$TOTAL" "Cenários de stress / failure (12)"
  why   "exercita concorrência, idempotência sob rajada, crash + redelivery, etc"
  ( cd "$ROOT_DIR" && bash scripts/stress.sh all ) || fail "stress.sh all não passou"
  pass "12 cenários de stress executados"
fi

# ─────────────────────────────────────────────────────────────────────
sep
printf '%s  ✓ DEMO COMPLETA — todos os requisitos do case exercitados%s\n' "$C_GRN$C_BLD" "$C_RST"
sep
echo
printf '  %sURLs disponíveis no browser:%s\n' "$C_BLD" "$C_RST"
echo  "    → $JAEGER_BASE          (Jaeger — explore os traces)"
echo  "    → $API_BASE/docs        (ReDoc — explore a API)"
echo  "    → $API_BASE/metrics/dev-1/summary   (curl direto)"
echo

if command -v open >/dev/null 2>&1; then
  open "$JAEGER_BASE" "$API_BASE/docs" 2>/dev/null || true
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$JAEGER_BASE" >/dev/null 2>&1 &
  xdg-open "$API_BASE/docs" >/dev/null 2>&1 &
fi

echo  "  Próximos passos:"
echo  "    make stress         # menu interativo de 12 cenários de falha"
[ "$FULL" -eq 0 ] && echo "    make demo-full      # demo + stress all (~7 min)"
echo  "    docker compose down -v"
echo
