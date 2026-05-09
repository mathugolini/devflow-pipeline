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

`make`, `aws` CLI, `jq` e `uuidgen` são opcionais — todos os scripts caem para Docker (`amazon/aws-cli`) automaticamente quando ausentes.

## Caminho rápido para o avaliador

3 níveis em ordem crescente de profundidade. Escolha o que faz sentido pro tempo disponível:

| Comando | Tempo | O que faz |
|---|---|---|
| `docker compose up -d` | ~12s | Sobe a stack inteira. Cumpre o requisito do case. |
| `make demo` | ~3 min | Sobe + popula + valida + abre Jaeger e ReDoc no browser, com narração e asserts em cada etapa. |
| `make demo-full` | ~7 min | `make demo` + 12 cenários de stress / chaos engineering. |

Pulou direto pra `make demo` se quiser experiência guiada. Para entender o que está rodando por trás, leia "Como rodar" abaixo.

## Como rodar (um único comando)

```bash
docker compose up -d
```

Isso é tudo. O `depends_on` orquestra a sequência:

1. `localstack` sobe e fica `healthy`.
2. `aws-init` (one-shot) roda `infra/localstack/init-aws.sh` — cria as 4 filas SQS (com `RedrivePolicy maxReceiveCount=3`) e as 2 tabelas DynamoDB (`events` com GSI + TTL, `developer_summary`). Idempotente.
3. `processor` e `aggregator` só sobem depois que o `aws-init` terminou com sucesso.
4. `jaeger` sobe em paralelo, expondo collector OTLP/HTTP em `:4318` e UI em `:16686`.

Boot completo em ~12s. URLs expostos:

| Serviço | URL |
| --- | --- |
| API REST do Aggregator | http://localhost:8080 |
| Documentação interativa (ReDoc) | http://localhost:8080/docs |
| Tracing UI (Jaeger) | http://localhost:16686 |
| LocalStack endpoint | http://localhost:4566 |

### Verificando que subiu certo

```bash
make verify-infra      # lista filas + tabelas
docker compose ps      # 5 containers up + aws-init exited(0)
```

### Para derrubar

```bash
docker compose down -v
```

### Atalhos via Makefile

`make up` é equivalente a `docker compose up -d` mais um `echo` dos URLs no final.
`make help` lista todos os outros alvos (`seed`, `verify-e2e`, `stress`, `logs`, `tools`, …).

## Decisões de design (vivo)

- **Dois `go.mod`** (um por serviço) — força contrato explícito via JSON na fila, sem acoplamento por pacote compartilhado.
- **`init-aws.sh` em container `aws-init` separado**, não em hook do LocalStack — isola o provisionamento, é idempotente e dá feedback claro no `docker compose`.
- **`PERSISTENCE=0`** no LocalStack — cada `docker compose up` parte de estado limpo. Simplifica demonstração.
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
| GET | `/openapi.yaml` | Contrato OpenAPI 3.1 (embed via `//go:embed`) |
| GET | `/docs` | UI interativa renderizada por ReDoc (carrega o YAML acima) |

Especificação fonte em [services/aggregator/internal/infra/api/openapi.yaml](services/aggregator/internal/infra/api/openapi.yaml). Editar lá → próximo build já serve a versão nova; sem geração via anotações em código.

### Exemplos

```bash
docker compose up -d                         # ou `make up`
bash scripts/seed.sh                         # ou `make seed` — ~21 eventos mistos
sleep 5
curl -s http://localhost:8080/health
curl -s http://localhost:8080/metrics/dev-1/summary
curl -s http://localhost:8080/metrics/dev-1
cd services/aggregator && go test ./...      # ou `make test-aggregator`
docker compose logs -f aggregator            # ou `make logs-aggregator`
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
docker compose up -d        # ou `make up`
make verify-e2e             # exit 0 quando tudo passa, exit 1 com mensagem clara em qualquer falha
```

### Tracing distribuído (OpenTelemetry)

Ambos os serviços instrumentam:

- HTTP server (otelhttp) — span por request entrante.
- AWS SDK (otelaws) — spans por chamada SQS/DynamoDB com timing real.
- SQS produtor/consumidor — spans manuais (`sqs.send processed-events`, `process processed-events`) com kind `Producer`/`Consumer`.
- **Propagação cross-service via `MessageAttributes` da SQS** (W3C `traceparent`) — o trace_id continua o mesmo do Processor para o Aggregator.

Cada log de processamento inclui o `trace_id` para pivot direto entre logs JSON e a UI do Jaeger:

```
{"msg":"processed", "event_id":"...", "trace_id":"37bd670620b77e689a8c844803fa1eb3"}
```

A boot do tracer usa as variáveis OTel padrão. Em `docker-compose.yml`:

```yaml
- OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318
- OTEL_EXPORTER_OTLP_INSECURE=true
```

Endpoint vazio → tracing silenciosamente desabilitado (TracerProvider noop). Em produção, troque o endpoint para um collector OTel.

```bash
docker compose up -d          # sobe Jaeger (16686) + serviços com tracing ligado
bash scripts/seed.sh
open http://localhost:16686   # ou `make jaeger`
```

Na UI, escolha o serviço `devflow-processor` ou `devflow-aggregator`, clique em "Find Traces" e abra qualquer trace para ver os 8 spans atravessando os dois serviços.

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
