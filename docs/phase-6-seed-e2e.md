# Fase 6 — Seed e validação end-to-end

## Objetivo

Provar que o pipeline inteiro funciona com **um comando**: do `send-message` na SQS `raw-events` até o `GET /metrics/{dev}/summary` na API REST. E provar que continua funcionando em re-runs, com idempotência e DLQ exercitadas.

Dois scripts, dois papéis:

- [`scripts/seed.sh`](scripts/seed.sh) — popula a fila com um mix realista de eventos.
- [`scripts/verify-e2e.sh`](scripts/verify-e2e.sh) — exerce o pipeline e faz asserts em cada estágio.

## `seed.sh`

Envia **~21 mensagens** para `raw-events`:

| Categoria | Quantidade | Para quê |
|---|---|---|
| `commit` válidos | 13 (dev-1: 7, dev-2: 6) | Carga base; alimenta `total_commits` no summary. |
| `pull_request` válidos | 5 | Cobre o segundo tipo de métrica. |
| `review_time` válidos | 3 (todos com `duration_minutes=30`) | Permite asserção precisa de `avg_review_time_minutes ≈ 30`. |
| `event_id` duplicado | 1 (mesma payload reenviada) | Prova idempotência via condicional `attribute_not_exists(event_id)`. |
| Eventos inválidos | 2 (tipo desconhecido + timestamp no futuro) | Vão para DLQ após `maxReceiveCount=3`. |

Idempotente entre execuções — re-rodar acumula histórico, mas o `verify-e2e` lida com isso via baseline.

Sem dependência de binário local: se `aws` CLI não está no PATH, cai automaticamente para `docker run amazon/aws-cli` na network do compose. Mesmo padrão de `init-aws.sh` e `verify-e2e.sh`.

## `verify-e2e.sh`

Sete estágios encadeados, cada um com mensagem clara em caso de falha:

1. **Pré-flight.** Confere as 4 filas SQS, as 2 tabelas DynamoDB, e `GET /health` da API. Falha imediata se a stack não estiver pronta.
2. **Baseline.** Captura `Scan Count` atual de `events` e `ApproximateNumberOfMessages` da DLQ — torna o script re-rodável sem `make down -v`.
3. **Seed.** Chama `scripts/seed.sh`.
4. **Drain (polling, sem `sleep` fixo).** Aguarda até que (a) `raw-events` tenha 0 mensagens visíveis E invisíveis, E (b) `events` tenha crescido `>= 19` em relação ao baseline. Timeout configurável via `TIMEOUT_SECONDS=` (default 60s; o `verify-e2e` do Makefile usa 120s para cobrir 3× `VisibilityTimeout` da DLQ).
5. **Asserts via API.**
   - `GET /metrics/dev-1/summary` → `total_commits >= 11`, `review_time_count >= 3`, `avg_review_time_minutes ≈ 30`.
   - `GET /metrics/dev-2/summary` → `total_pull_requests >= 5`.
   - `GET /metrics/dev-1` → lista não-vazia.
6. **DLQ.** Confere que `raw-events-dlq` cresceu `>= 2` (os 2 inválidos depois de 3 redeliveries).
7. **Idempotência ao vivo.** Gera um `dev-{uuid}` efêmero, envia 3× o mesmo `event_id`, e prova que `events` cresceu exatamente 1 e o summary do dev marca `total_commits=1`, `events_processed=1`.

Saída coloreada (`[verify] PASS:` verde, `FAIL:` vermelho) e `exit 1` no primeiro problema.

## Como rodar

```bash
docker compose up -d        # ou `make up`
make verify-e2e             # script completo, exit 0 quando tudo passa
```

Variáveis úteis:

- `TIMEOUT_SECONDS=120` — aumenta o drain (default no Makefile).
- `API_BASE=http://localhost:8080` — apontar para outro host.
- `AWS_ENDPOINT_URL` — apontar para um LocalStack remoto.

## Por que assim

- **Polling em vez de `sleep` fixo.** `sleep 30` é o pior dos mundos: lento quando a máquina é rápida, flaky quando é lenta. O loop de drain termina assim que as duas condições batem.
- **Asserts contra a API, não contra o DynamoDB.** O contrato com o usuário final é a API. Se o handler agrega errado, o teste pega — mesmo que o DynamoDB esteja certo.
- **Idempotência exercitada ao vivo.** O unit test de idempotência prova a lógica; aqui provamos que o stack inteiro (incluindo a `ConditionalCheckFailedException` real do DynamoDB) honra o contrato.
- **DLQ como sinal positivo.** Mensagens malformadas indo para DLQ é comportamento esperado, não erro — o `verify-e2e` afirma o crescimento explicitamente.
