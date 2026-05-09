# devflow-pipeline

Pipeline de métricas de produtividade de desenvolvedores em Go.
Dois serviços comunicando via SQS, persistindo em DynamoDB, expostos via API REST. Tudo roda em LocalStack.

```
[SQS: raw-events] → [Processor] → [SQS: processed-events] → [Aggregator] → [DynamoDB] → [API REST]
```

## Status atual

- [x] Fase 1 — Infra (LocalStack + provisionamento automático de SQS/DynamoDB) — [doc](docs/phase-1-infrastructure.md)
- [x] Fase 2 — Processor (validação, worker pool, graceful shutdown, Dockerfile distroless) — [doc](docs/phase-2-processor.md)
- [x] Fase 3 — Aggregator (DynamoDB + REST API) — [doc](docs/phase-3-aggregator.md)
- [x] Fase 4 — Dockerfiles multi-stage (distroless `nonroot`, `CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`)
- [x] Fase 5 — Testes (domínio + use case mockado + handlers `httptest`)
- [x] Fase 6 — Seed e validação end-to-end (`make seed`, `make verify-e2e`)

## Pré-requisitos

- Docker + Docker Compose v2
- `make`
- `aws` CLI (opcional, só para os comandos de verificação manual)

## Subindo a infraestrutura (Fase 1)

```bash
make up
```

O alvo:
1. Sobe o container `localstack` e aguarda o healthcheck.
2. Roda o container `aws-init` (one-shot) que executa `infra/localstack/init-aws.sh`, criando filas e tabelas. O script é idempotente.

### Verificando

```bash
make verify-infra
```

Saída esperada: 4 filas (`raw-events`, `raw-events-dlq`, `processed-events`, `processed-events-dlq`), 2 tabelas (`events`, `developer_summary`) e a `RedrivePolicy` da `raw-events` apontando para a DLQ com `maxReceiveCount=3`.

### Derrubando

```bash
make down
```

## Decisões de design (vivo)

- **Dois `go.mod`** (um por serviço) — força contrato explícito via JSON na fila, sem acoplamento por pacote compartilhado.
- **`init-aws.sh` em container `aws-init` separado**, não em hook do LocalStack — isola o provisionamento, é idempotente e dá feedback claro no `docker compose`.
- **`PERSISTENCE=0`** no LocalStack — cada `make up` parte de estado limpo. Simplifica demonstração.
- **DLQ + `maxReceiveCount=3`** cuidam do retry de mensagens inválidas — o código Go não reimplementa essa lógica. Retry com backoff no código fica reservado para falhas de infraestrutura (SDK errors).

## Aggregator (Fase 3)

O segundo serviço consome `processed-events`, persiste cada evento em
DynamoDB (`events`) e mantém uma projeção por desenvolvedor
(`developer_summary`). Expõe uma API REST somente leitura na porta
**8080** (sem autenticação — apenas para a demo; o router é
middleware-ready para adicionar auth sem refactor).

Decisões fechadas em [docs/phase-3-decisions.md](docs/phase-3-decisions.md);
implementação detalhada em [docs/phase-3-aggregator.md](docs/phase-3-aggregator.md).

### Endpoints

| Método | Path | Descrição |
| --- | --- | --- |
| GET | `/health` | 200 ok / 503 com detalhes de SQS + DynamoDB |
| GET | `/metrics/{developer_id}` | Lista os eventos persistidos do dev (Query no GSI `developer_id-index`) |
| GET | `/metrics/{developer_id}/summary` | Agregado: totais + `avg_review_time_minutes` calculado no handler |

### Exemplos

```bash
make up                                      # infra + processor + aggregator
make seed                                    # ~21 eventos (válidos + inválidos + duplicado)
sleep 5
curl -s http://localhost:8080/health
curl -s http://localhost:8080/metrics/dev-1/summary
curl -s http://localhost:8080/metrics/dev-1
make test-aggregator                         # roda os unit tests do serviço
make logs-aggregator                         # tail dos logs JSON
```

### Validação end-to-end

`make verify-e2e` exercita o pipeline inteiro contra a stack rodando e
falha rápido com mensagem clara se algum estágio quebrar. O script
([scripts/verify-e2e.sh](scripts/verify-e2e.sh)) faz:

1. **Pré-flight** — confere que as 4 filas, as 2 tabelas e o `/health`
   da API respondem.
2. **Baseline** — captura a contagem atual de `events` e da DLQ
   `raw-events-dlq` para que o script seja seguro de re-rodar.
3. **Seed** — chama `scripts/seed.sh` (~21 eventos: 19 válidos + 2
   inválidos, com 1 `event_id` duplicado).
4. **Drain** — faz polling em `raw-events` até `ApproximateNumberOfMessages`
   e `…NotVisible` zerarem **e** a tabela `events` ter crescido `>= 19`.
   Sem `sleep` fixo — timeout configurável via `TIMEOUT_SECONDS=`.
5. **Assertions de persistência** — chama `GET /metrics/dev-1/summary`
   e `…/dev-2/summary`, valida `total_commits >= 11`, `review_time_count >= 3`,
   `avg_review_time_minutes ≈ 30`, `total_pull_requests >= 5`. Valida que
   `GET /metrics/dev-1` lista eventos.
6. **DLQ** — confere que `raw-events-dlq` cresceu `>= 2` (os inválidos do seed
   após `maxReceiveCount=3`).
7. **Idempotência ao vivo** — envia 3x o mesmo `event_id` e prova que
   a tabela `events` cresce exatamente 1, e que o `developer_summary`
   do dev efêmero registra `total_commits=1`, `events_processed=1`.

```bash
make up
make verify-e2e   # exit 0 quando tudo passa, exit 1 com mensagem clara em qualquer falha
```

### Notas operacionais

- **Hot partition.** `developer_id` é partition key em `developer_summary`. Em
  PAY_PER_REQUEST a AWS rebalanceia partições automaticamente, mas um produtor
  service-account pode causar throttling. Mitigação: sharding artificial ou
  cache (Redis) com flush periódico — fora do escopo desta demo.
- **API sem auth.** A API é pública na porta 8080 propositalmente — qualquer
  ambiente compartilhado precisa colocar a API atrás de um API Gateway com IAM
  ou um service mesh com mTLS. O `NewRouter(handler, mws...)` aceita
  middlewares variádicos para tornar isso uma mudança de uma linha.
- **TTL.** Cada row em `events` tem `expires_at = processed_at + 30d`.
  Idempotência é eterna durante a janela e a tabela não cresce sem fim.
