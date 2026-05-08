#!/usr/bin/env bash
# Stress / failure scenarios for the devflow pipeline.
#
# Run with: bash scripts/stress.sh [scenario_number|all]
#   bash scripts/stress.sh        # interactive menu
#   bash scripts/stress.sh all    # run every scenario in sequence
#   bash scripts/stress.sh 1      # run only scenario 1
#
# Requires: docker stack already up (`make up`), aws-cli installed locally
# OR Docker (script auto-falls back to running aws-cli in a container).
set -uo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:4566}"
API="${API:-http://localhost:8080}"
QUEUE_RAW="$ENDPOINT/000000000000/raw-events"
QUEUE_PROC="$ENDPOINT/000000000000/processed-events"
DLQ_RAW="$ENDPOINT/000000000000/raw-events-dlq"
DLQ_PROC="$ENDPOINT/000000000000/processed-events-dlq"

# ── colors ──────────────────────────────────────────────────────────
if [ -t 1 ]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'
  C_BLU=$'\033[34m'; C_DIM=$'\033[2m';  C_BLD=$'\033[1m'; C_RST=$'\033[0m'
else
  C_RED=""; C_GRN=""; C_YEL=""; C_BLU=""; C_DIM=""; C_BLD=""; C_RST=""
fi

# ── aws cli wrapper (local or dockerized) ───────────────────────────
if command -v aws >/dev/null 2>&1; then
  aws_cmd() { aws --endpoint-url="$ENDPOINT" --region us-east-1 "$@"; }
else
  aws_cmd() {
    docker run --rm --network host \
      -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
      amazon/aws-cli:2.17.0 --endpoint-url="$ENDPOINT" --region us-east-1 "$@"
  }
fi

uuid() { uuidgen | tr 'A-Z' 'a-z'; }
# Use a timestamp ~60s in the past. Avoids spurious "timestamp in the future"
# rejections caused by Docker Desktop VM clock drift after the host laptop sleeps.
# BSD date (macOS) uses -v; GNU date (Linux) uses -d.
now() {
  date -u -v-60S +%FT%TZ 2>/dev/null \
    || date -u -d '-60 seconds' +%FT%TZ 2>/dev/null \
    || date -u +%FT%TZ
}

send_raw() {
  aws_cmd sqs send-message --queue-url "$QUEUE_RAW" --message-body "$1" >/dev/null
}
send_processed() {
  aws_cmd sqs send-message --queue-url "$QUEUE_PROC" --message-body "$1" >/dev/null
}
qsize() {
  aws_cmd sqs get-queue-attributes --queue-url "$1" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo "?"
}
summary_field() {
  local dev="$1" field="$2"
  curl -sS "$API/metrics/$dev/summary" | jq -r ".$field // 0" 2>/dev/null || echo "0"
}
http_status() {
  curl -s -o /dev/null -w "%{http_code}" "$1"
}

# Wait until both raw-events and processed-events queues drain (or timeout).
# Used between scenarios so backlog from a previous burst doesn't poison the next.
wait_for_drain() {
  local timeout="${1:-120}"
  local i=0
  while [ "$i" -lt "$timeout" ]; do
    local r p
    r=$(qsize "http://localhost:4566/000000000000/raw-events" 2>/dev/null || echo "?")
    p=$(qsize "http://localhost:4566/000000000000/processed-events" 2>/dev/null || echo "?")
    if [ "$r" = "0" ] && [ "$p" = "0" ]; then return 0; fi
    sleep 2; i=$((i+2))
    printf "\r  ${C_DIM}aguardando filas drenarem (raw=%s proc=%s t=%ds)${C_RST}" "$r" "$p" "$i"
  done
  echo
  return 1
}

banner() {
  echo
  echo "${C_BLU}${C_BLD}━━━ Scenario $1: $2 ━━━${C_RST}"
  [ -n "${3:-}" ] && echo "${C_DIM}$3${C_RST}"
}
pass() { echo "  ${C_GRN}✓ PASS${C_RST} $1"; }
fail() { echo "  ${C_RED}✗ FAIL${C_RST} $1"; FAILS=$((FAILS+1)); }
info() { echo "  ${C_DIM}· $1${C_RST}"; }

FAILS=0

# ── helpers for assertions ──────────────────────────────────────────
assert_eq() {
  local got="$1" want="$2" desc="$3"
  if [ "$got" = "$want" ]; then pass "$desc (=$got)"
  else fail "$desc — expected $want, got $got"; fi
}
assert_ge() {
  local got="$1" min="$2" desc="$3"
  if [ "$got" -ge "$min" ] 2>/dev/null; then pass "$desc (=$got, ≥$min)"
  else fail "$desc — expected ≥$min, got $got"; fi
}

# ── scenarios ───────────────────────────────────────────────────────

scenario_1() {
  banner 1 "Idempotência sob rajada concorrente" \
    "Mesma event_id enviada 20× em paralelo → deve contar 1 vez (D5 idempotência)."
  local eid; eid=$(uuid)
  local body; body=$(printf '{"event_id":"%s","developer_id":"dev-stress-1","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s"}' "$eid" "$(now)")
  # send 20× em paralelo. Sem subshell () — wait precisa enxergar os jobs.
  for _ in $(seq 1 20); do send_raw "$body" & done
  wait
  info "Aguardando processamento (até 30s)..."
  local n=0
  for i in $(seq 1 15); do
    sleep 2
    n=$(summary_field dev-stress-1 events_processed)
    printf "\r  ${C_DIM}events_processed=%s (t=%ds)${C_RST}" "$n" "$((i*2))"
    [ "$n" = "1" ] && break
  done
  echo
  if [ "$n" = "0" ]; then
    fail "events_processed=0 — nenhuma mensagem chegou ao DDB. Verifique:"
    info "  docker logs devflow-processor 2>&1 | tail -20"
    info "  qsize raw-events-dlq: $(qsize "$DLQ_RAW")  (se ≥ 20, processor rejeitou)"
  else
    local c; c=$(summary_field dev-stress-1 total_commits)
    assert_eq "$n" "1" "events_processed deve ser 1"
    assert_eq "$c" "1" "total_commits deve ser 1"
  fi
}

scenario_2() {
  banner 2 "Validação + DLQ raw-events" \
    "Mensagens inválidas devem ir para raw-events-dlq após redrive (maxReceiveCount=3)."
  local before; before=$(qsize "$DLQ_RAW")
  send_raw '{"event_id":"not-a-uuid","developer_id":"x","metric_type":"commits","value":1,"repository":"o/r","timestamp":"2026-05-08T00:00:00Z"}'
  send_raw "$(printf '{"event_id":"%s","developer_id":"x","metric_type":"unknown","value":1,"repository":"o/r","timestamp":"2026-05-08T00:00:00Z"}' "$(uuid)")"
  send_raw "$(printf '{"event_id":"%s","developer_id":"x","metric_type":"commits","value":-5,"repository":"o/r","timestamp":"2026-05-08T00:00:00Z"}' "$(uuid)")"
  send_raw "$(printf '{"event_id":"%s","developer_id":"x","metric_type":"review_time_minutes","value":2000,"repository":"o/r","timestamp":"2026-05-08T00:00:00Z"}' "$(uuid)")"
  send_raw "$(printf '{"event_id":"%s","developer_id":"x","metric_type":"commits","value":1,"repository":"o/r","timestamp":"2099-01-01T00:00:00Z"}' "$(uuid)")"
  info "Aguardando 3 redeliveries × VisibilityTimeout=30s ≈ 100s..."
  for i in $(seq 1 25); do
    sleep 5
    local cur; cur=$(qsize "$DLQ_RAW")
    printf "\r  ${C_DIM}DLQ raw: %s (t=%ds)${C_RST}" "$cur" "$((i*5))"
    [ "$cur" -ge "$((before+5))" ] 2>/dev/null && break
  done
  echo
  local after; after=$(qsize "$DLQ_RAW")
  assert_ge "$((after-before))" "5" "5 mensagens chegaram em raw-events-dlq"
}

scenario_3() {
  banner 3 "JSON malformado / campos desconhecidos" \
    "DisallowUnknownFields rejeita; não-JSON é decode error → tudo vai pra DLQ."
  local before; before=$(qsize "$DLQ_RAW")
  send_raw 'isto-nao-eh-json'
  send_raw "$(printf '{"event_id":"%s","developer_id":"d","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s","campo_extra":"hack"}' "$(uuid)" "$(now)")"
  info "Aguardando redrive..."
  sleep 100
  local after; after=$(qsize "$DLQ_RAW")
  assert_ge "$((after-before))" "2" "2 mensagens malformadas em DLQ"
}

scenario_4() {
  banner 4 "Schema version desconhecida (D1)" \
    "Mensagem com schema_version=99 publicada direto em processed-events deve ir pra processed-events-dlq."
  local before; before=$(qsize "$DLQ_PROC")
  local eid; eid=$(uuid)
  send_processed "$(printf '{"schema_version":99,"event_id":"%s","developer_id":"d-bad","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s","processed_at":"%s","processor_id":"fake"}' "$eid" "$(now)" "$(now)")"
  info "Aguardando redrive (≈100s)..."
  sleep 100
  local after; after=$(qsize "$DLQ_PROC")
  assert_ge "$((after-before))" "1" "Mensagem com schema desconhecido em processed-events-dlq"
}

scenario_5() {
  banner 5 "Throughput burst — 200 mensagens" \
    "Mede latência fim-a-fim e prova que worker pool não perde mensagens."
  wait_for_drain 60 || true; echo
  local start; start=$(date +%s)
  for i in $(seq 1 200); do
    local eid; eid=$(uuid)
    send_raw "$(printf '{"event_id":"%s","developer_id":"dev-burst","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s"}' "$eid" "$(now)")" &
    if (( i % 25 == 0 )); then wait; fi
  done; wait
  info "Send levou $(( $(date +%s) - start ))s. Aguardando agregação (até 180s)..."
  local n=0
  for i in $(seq 1 90); do
    n=$(summary_field dev-burst events_processed)
    printf "\r  ${C_DIM}events_processed=%s (t=%ds)${C_RST}" "$n" "$((i*2))"
    [ "$n" = "200" ] && break
    sleep 2
  done
  echo
  local total; total=$(( $(date +%s) - start ))
  assert_eq "$n" "200" "todas as 200 mensagens agregadas"
  info "tempo total: ${total}s"
}

scenario_6() {
  banner 6 "Crash do aggregator + redelivery" \
    "Mata o container no meio do processamento; após restart, idempotência deve segurar a contagem."
  wait_for_drain 60 || true; echo
  for i in $(seq 1 60); do
    local eid; eid=$(uuid)
    send_raw "$(printf '{"event_id":"%s","developer_id":"dev-crash","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s"}' "$eid" "$(now)")" &
    (( i % 20 == 0 )) && wait
  done; wait
  sleep 2
  info "kill -9 no aggregator..."
  docker kill devflow-aggregator >/dev/null
  sleep 3
  info "subindo de novo..."
  docker compose up -d aggregator >/dev/null 2>&1
  info "Aguardando recovery (até 120s)..."
  local n=0
  for i in $(seq 1 60); do
    sleep 2
    n=$(summary_field dev-crash events_processed)
    printf "\r  ${C_DIM}events_processed=%s (t=%ds)${C_RST}" "$n" "$((i*2))"
    [ "$n" = "60" ] && break
  done
  echo
  assert_eq "$n" "60" "todas as 60 mensagens contabilizadas exatamente 1×"
}

scenario_7() {
  banner 7 "Graceful shutdown drena workers" \
    "SIGTERM via 'docker stop' enquanto há trabalho — drena, não perde, não duplica."
  wait_for_drain 60 || true; echo
  for i in $(seq 1 100); do
    local eid; eid=$(uuid)
    send_raw "$(printf '{"event_id":"%s","developer_id":"dev-graceful","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s"}' "$eid" "$(now)")" &
    (( i % 25 == 0 )) && wait
  done; wait
  sleep 1
  info "docker stop (SIGTERM)..."
  docker stop devflow-aggregator >/dev/null
  docker compose up -d aggregator >/dev/null 2>&1
  info "Aguardando convergência (até 120s)..."
  local n=0
  for i in $(seq 1 60); do
    sleep 2
    n=$(summary_field dev-graceful events_processed)
    printf "\r  ${C_DIM}events_processed=%s (t=%ds)${C_RST}" "$n" "$((i*2))"
    [ "$n" = "100" ] && break
  done
  echo
  assert_eq "$n" "100" "100 mensagens contabilizadas após shutdown gracioso"
}

scenario_8() {
  banner 8 "Visibility timeout exaurido" \
    "Pause o container > 30s → SQS reentrega → idempotência deve segurar."
  wait_for_drain 60 || true; echo
  local eid; eid=$(uuid)
  send_raw "$(printf '{"event_id":"%s","developer_id":"dev-vt","metric_type":"commits","value":1,"repository":"o/r","timestamp":"%s"}' "$eid" "$(now)")"
  sleep 1
  info "pausando aggregator por 35s..."
  docker pause devflow-aggregator >/dev/null
  sleep 35
  docker unpause devflow-aggregator >/dev/null
  info "Aguardando agregação..."
  local n=0
  for i in $(seq 1 30); do
    sleep 2
    n=$(summary_field dev-vt events_processed)
    [ "$n" = "1" ] && break
  done
  assert_eq "$n" "1" "events_processed=1 mesmo apos reentrega"
  local seen=0
  seen=$(docker logs devflow-aggregator 2>&1 | grep -c "$eid" || echo 0)
  info "log mostrou event_id ${seen} vezes (esperado >=2 entregas, 1 commit)"
}

scenario_9() {
  banner 9 "API durante outage do DDB" \
    "Para localstack → /health deve retornar 503 com checks descritivos."
  info "parando localstack..."
  docker stop devflow-localstack >/dev/null
  sleep 3
  local code; code=$(http_status "$API/health")
  if [ "$code" = "503" ]; then pass "/health respondeu 503 com DDB fora"
  else fail "/health respondeu $code (esperado 503)"; fi
  info "subindo de novo..."
  docker start devflow-localstack >/dev/null
  sleep 8
  echo "  ${C_YEL}!${C_RST} Lembre: PERSISTENCE=0 — após restart as filas/tabelas foram apagadas."
  echo "  ${C_YEL}!${C_RST} Rode: ${C_BLD}make up${C_RST} pra reprovisionar antes do próximo cenário."
}

scenario_10() {
  banner 10 "Eventos fora de ordem (limitação documentada)" \
    "Demonstra que last_activity é SET incondicional → pode voltar no tempo."
  local dev=dev-order
  send_raw "$(printf '{"event_id":"%s","developer_id":"%s","metric_type":"commits","value":1,"repository":"o/r","timestamp":"2026-05-07T23:00:00Z"}' "$(uuid)" "$dev")"
  sleep 4
  send_raw "$(printf '{"event_id":"%s","developer_id":"%s","metric_type":"commits","value":1,"repository":"o/r","timestamp":"2026-05-07T20:00:00Z"}' "$(uuid)" "$dev")"
  sleep 6
  local last; last=$(summary_field "$dev" last_activity)
  local total; total=$(summary_field "$dev" total_commits)
  assert_eq "$total" "2" "total_commits=2 (contagem correta)"
  if [ "$last" = "2026-05-07T20:00:00Z" ]; then
    echo "  ${C_YEL}⚠ EXPECTED LIMITATION${C_RST} last_activity voltou no tempo: $last"
    echo "  ${C_DIM}fix futuro: ConditionExpression last_activity < :ts (D4 trade-off documentado)${C_RST}"
  else
    info "last_activity=$last (não voltou — eventos foram processados em ordem inversa)"
  fi
}

scenario_11() {
  banner 11 "Edge cases da API" \
    "Endpoints com developer inexistente / ID gigante / path edge."
  local code
  code=$(http_status "$API/metrics/nao-existe/summary")
  assert_eq "$code" "404" "summary de developer inexistente → 404"
  code=$(http_status "$API/metrics/nao-existe")
  assert_eq "$code" "200" "events de developer inexistente → 200 com []"
  local body; body=$(curl -s "$API/metrics/nao-existe")
  assert_eq "$body" "[]" "  body deve ser []"
}

scenario_12() {
  banner 12 "Payload limítrofe — repository > 256 chars" \
    "Validação de tamanho rejeita → DLQ."
  local big; big=$(printf 'x%.0s' {1..300})
  local before; before=$(qsize "$DLQ_RAW")
  send_raw "$(printf '{"event_id":"%s","developer_id":"d","metric_type":"commits","value":1,"repository":"%s","timestamp":"%s"}' "$(uuid)" "$big" "$(now)")"
  info "Aguardando redrive..."
  sleep 100
  local after; after=$(qsize "$DLQ_RAW")
  assert_ge "$((after-before))" "1" "repository > 256 chars rejeitado"
}

run_all() {
  for n in 1 2 3 4 5 6 7 8 10 11 12; do "scenario_$n" || true; done
  echo
  echo "${C_BLD}━━━ Sumário ━━━${C_RST}"
  if [ "$FAILS" -eq 0 ]; then
    echo "${C_GRN}${C_BLD}Todos os cenários passaram.${C_RST}"
  else
    echo "${C_RED}${C_BLD}$FAILS asserções falharam.${C_RST}"
    exit 1
  fi
}

show_menu() {
  cat <<EOF
${C_BLD}DevFlow stress harness${C_RST}

  ${C_BLD}1${C_RST}  Idempotência sob rajada concorrente  (rápido, ~10s)
  ${C_BLD}2${C_RST}  Validação + DLQ raw-events            (lento, ~100s)
  ${C_BLD}3${C_RST}  JSON malformado                       (lento, ~100s)
  ${C_BLD}4${C_RST}  Schema version desconhecida (D1)      (lento, ~100s)
  ${C_BLD}5${C_RST}  Throughput burst — 200 msgs          (médio, ~60s)
  ${C_BLD}6${C_RST}  Crash do aggregator                   (médio, ~75s)
  ${C_BLD}7${C_RST}  Graceful shutdown                     (médio, ~50s)
  ${C_BLD}8${C_RST}  Visibility timeout exaurido           (médio, ~50s)
  ${C_BLD}9${C_RST}  Outage do DDB                         (rápido — destrutivo: requer make up depois)
  ${C_BLD}10${C_RST} Eventos fora de ordem                  (rápido)
  ${C_BLD}11${C_RST} Edge cases da API                     (rápido)
  ${C_BLD}12${C_RST} Payload > 256 chars                   (lento, ~100s)
  ${C_BLD}all${C_RST}  Roda 1-8, 10-12 em sequência (~10min, pula o 9)
  ${C_BLD}q${C_RST}    Sair
EOF
  read -rp "$(echo "${C_BLD}escolha:${C_RST} ")" choice
  case "$choice" in
    [0-9]|1[0-2]) "scenario_$choice"; show_menu ;;
    all) run_all ;;
    q|Q|"") exit 0 ;;
    *) echo "opção inválida"; show_menu ;;
  esac
}

# ── pré-checks ──────────────────────────────────────────────────────
preflight() {
  if ! curl -sf "$API/health" >/dev/null 2>&1; then
    echo "${C_RED}!!  Aggregator em $API não responde — rode 'make up' primeiro.${C_RST}" >&2
    exit 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "${C_RED}!!  'jq' não encontrado — instale (brew install jq).${C_RST}" >&2
    exit 1
  fi
}

preflight

case "${1:-}" in
  ""|menu) show_menu ;;
  all) run_all ;;
  [0-9]|1[0-2]) "scenario_$1" ;;
  *) echo "uso: $0 [1-12|all|menu]"; exit 1 ;;
esac
