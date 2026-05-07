# Decisões de design — Fase 3 (Aggregator + DynamoDB + API REST)

> Documento de decisões fechadas **antes** de escrever código. Cada item: contexto, opções consideradas, decisão, justificativa, e como será implementado.
>
> Origem: itens D1-D6 de [docs/security-and-review-v2.md](security-and-review-v2.md).

---

## D1. Schema versioning na mensagem da fila

**Contexto.** O contrato JSON entre Processor e Aggregator não tem campo de versão. Mudar o formato no futuro exige deploy coordenado dos dois serviços.

**Opções.**
| | Prós | Contras |
| --- | --- | --- |
| Sem versão | Simples agora | Refactor caro depois; código condicional crônico |
| `schema_version: 1` | 1 linha hoje, evolução segura | Nenhum custo real |
| Schema externo (Avro/Protobuf) | Tipagem forte cross-language | Overkill pro escopo; complexity tax |

**Decisão.** Adicionar `schema_version: 1` na `ProcessedEvent`. Aggregator rejeita versões desconhecidas → DLQ.

**Implementação.**
```go
// services/processor/internal/domain/event.go
const CurrentSchemaVersion = 1

type ProcessedEvent struct {
    SchemaVersion int `json:"schema_version"`
    RawEvent
    ProcessedAt time.Time `json:"processed_at"`
    ProcessorID string    `json:"processor_id"`
}
```

```go
// services/aggregator/internal/usecase/aggregate_event.go
if evt.SchemaVersion != domain.SupportedSchemaVersion {
    return &domain.UnsupportedSchemaError{Got: evt.SchemaVersion, Want: domain.SupportedSchemaVersion}
}
```

**Trade-off explícito.** Adicionar o campo agora invalida mensagens já em vôo na `processed-events` que não tenham `schema_version` — e a estratégia `DisallowUnknownFields` (PR #6) torna isso ainda mais estrito. Como estamos antes do Aggregator existir, nenhuma mensagem real vai sofrer. Na vida real isso seria um migration script.

---

## D2. Idempotência: janela de tempo

**Contexto.** O Aggregator vai usar `ConditionExpression: attribute_not_exists(event_id)` na tabela `events`. Isso garante idempotência **eterna** enquanto a row existir. Sem TTL, a tabela cresce monotonicamente.

**Opções.**
1. **Sem TTL** — idempotência perfeita, custo crescente. Bom para audit eterno.
2. **TTL 30 dias** — cobre 100% dos cenários reais (SQS dedup window é 5 min, redelivery max é minutos). Custo controlado.
3. **TTL 7 dias** — mais agressivo. Cobre cenários normais; incidentes longos podem perder a janela.

**Decisão.** **TTL de 30 dias**. Implementado via atributo `expires_at` na tabela `events` + DynamoDB TTL feature.

**Justificativa.** SQS standard tem `MessageRetentionPeriod` de até 14 dias. Uma mensagem que sobrevive 30 dias é cenário inexistente. 30 dias dá margem confortável + audit trail aceitável. Quem precisar de retenção maior, joga via DynamoDB Streams pra S3 (frase pra README — fora de escopo).

**Implementação.**
```go
type EventRecord struct {
    EventID     string  `dynamodbav:"event_id"`
    DeveloperID string  `dynamodbav:"developer_id"`
    MetricType  string  `dynamodbav:"metric_type"`
    Value       float64 `dynamodbav:"value"`
    Repository  string  `dynamodbav:"repository"`
    Timestamp   string  `dynamodbav:"timestamp"`
    ProcessedAt string  `dynamodbav:"processed_at"`
    ProcessorID string  `dynamodbav:"processor_id"`
    ExpiresAt   int64   `dynamodbav:"expires_at"` // Unix epoch seconds
}
```

E em `init-aws.sh`:
```bash
aws dynamodb update-time-to-live \
  --table-name events \
  --time-to-live-specification "Enabled=true,AttributeName=expires_at"
```

`ExpiresAt = ProcessedAt.Add(30 * 24 * time.Hour).Unix()`.

---

## D3. Hot partition risk no `developer_summary`

**Contexto.** `developer_id` é partition key. Se um dev (ex.: bot, service account) gera 100x mais eventos que a média, todas as escritas vão pra mesma partição → throttling.

**Opções.**
1. **Aceitar e monitorar.** Em PAY_PER_REQUEST mode, DynamoDB faz auto-scaling de partições. CloudWatch alarme em throttled requests.
2. **Sharding artificial** (`developer_id#shard0..N`). Espalha writes; complica reads (fanout/aggregate).
3. **Buffer + flush periódico no Aggregator.** Acumula em memória, flush a cada 5s. Reduz QPS por dev. Compromete idempotência simples.

**Decisão.** **Opção 1** (aceitar e monitorar) com nota explícita.

**Justificativa.**
- PAY_PER_REQUEST mode (o que estamos usando) já mitiga: AWS rebalanceia partições automaticamente até ~1000 WCU/partição.
- Para o case (demo + LocalStack), throughput não é um problema mensurável.
- Sharding artificial introduz complexidade de leitura que **não é exigida pelo case**.

**Como documentar.** Seção em `docs/phase-3-aggregator.md` listando "limitações conhecidas". Em prod com volume real, o caminho seria avaliar shard-on-demand ou migrar a coluna `events_processed`/contadores pra um cache (Redis) com flush periódico.

---

## D4. `avg_review_time_minutes` — armazenar componentes, não a média

**Contexto.** Decidir agora pra evitar refactor de schema depois.

**Risco da média pré-calculada.**
- Drift de ponto flutuante em incrementos.
- Impossibilidade de corrigir um evento individual (ex.: rollback).
- Não dá pra recalcular com filtros (ex.: "média dos últimos 7 dias").

**Decisão.** Schema da tabela `developer_summary`:

```
{
  developer_id:                 PK (string)
  total_commits:                number  // ADD para incrementos atômicos
  total_pull_requests:          number
  total_review_time_minutes:    number
  review_time_count:            number
  events_processed:             number
  last_activity:                string (ISO 8601)
  updated_at:                   string (ISO 8601)
}
```

API REST calcula `avg_review_time_minutes = total_review_time_minutes / review_time_count` no handler, com guarda `if review_time_count == 0 { return 0 }`.

**Implementação.**
```go
type DeveloperSummary struct {
    DeveloperID            string  `dynamodbav:"developer_id"`
    TotalCommits           int64   `dynamodbav:"total_commits"`
    TotalPullRequests      int64   `dynamodbav:"total_pull_requests"`
    TotalReviewTimeMinutes float64 `dynamodbav:"total_review_time_minutes"`
    ReviewTimeCount        int64   `dynamodbav:"review_time_count"`
    EventsProcessed        int64   `dynamodbav:"events_processed"`
    LastActivity           string  `dynamodbav:"last_activity"`
    UpdatedAt              string  `dynamodbav:"updated_at"`
}

func (s DeveloperSummary) AvgReviewTimeMinutes() float64 {
    if s.ReviewTimeCount == 0 {
        return 0
    }
    return s.TotalReviewTimeMinutes / float64(s.ReviewTimeCount)
}
```

---

## D5. Atomicidade entre `events.PutItem` e `developer_summary.UpdateItem`

**Contexto.** Cada evento gera duas escritas: persistir o evento + incrementar o summary. Se a primeira sucede e a segunda falha (rede, throttle), a próxima retry vai bater em `ConditionalCheckFailedException` no `events.PutItem` (já existe), e podemos interpretar como "já processado" → summary nunca atualizado. Bug silencioso.

**Opções.**
1. **Retornar erro do handler** sem cuidado especial → SQS redrive → race com idempotência → bug latente.
2. **Idempotência interna no summary** (set de processed event IDs por developer) → payload cresce sem teto.
3. **`TransactWriteItems`** cobrindo as duas escritas → atômico, custo 2x WCU.

**Decisão.** **Opção 3** — `TransactWriteItems`.

**Justificativa.**
- Elimina uma classe inteira de bug.
- 2x WCU é trivial em PAY_PER_REQUEST.
- O AWS SDK v2 expõe `TransactWriteItems` cleanly.

**Implementação (esboço).**
```go
input := &dynamodb.TransactWriteItemsInput{
    TransactItems: []types.TransactWriteItem{
        {
            Put: &types.Put{
                TableName: aws.String("events"),
                Item: eventItem,
                ConditionExpression: aws.String("attribute_not_exists(event_id)"),
            },
        },
        {
            Update: &types.Update{
                TableName: aws.String("developer_summary"),
                Key: map[string]types.AttributeValue{
                    "developer_id": &types.AttributeValueMemberS{Value: developerID},
                },
                UpdateExpression: aws.String(updateExpr),
                ExpressionAttributeValues: exprValues,
            },
        },
    },
}
_, err := client.TransactWriteItems(ctx, input)
if err != nil {
    var canceled *types.TransactionCanceledException
    if errors.As(err, &canceled) {
        // Inspect cancellation reasons. If [0] is "ConditionalCheckFailed",
        // the event already existed (idempotent retry) — no error.
        for i, r := range canceled.CancellationReasons {
            if i == 0 && r.Code != nil && *r.Code == "ConditionalCheckFailed" {
                return nil // already processed
            }
        }
    }
    return fmt.Errorf("transact write: %w", err)
}
```

A `UpdateExpression` usa `ADD` para os contadores e `SET` para `last_activity`/`updated_at`:
```
ADD total_commits :inc_c,
    total_pull_requests :inc_pr,
    total_review_time_minutes :inc_rtm,
    review_time_count :inc_rtc,
    events_processed :one
SET last_activity = if_not_exists(last_activity, :ts),
    updated_at = :now
```

**Detalhe importante.** Os incrementos `:inc_c`, `:inc_pr`, etc. são `0` ou `value` dependendo do `metric_type` do evento. Computado no use case antes da chamada.

---

## D6. API REST sem autenticação

**Contexto.** A API expõe métricas individuais. PII real. Em qualquer ambiente compartilhado, sem auth = vazamento.

**Decisão.** Documentar no README como decisão consciente de demo + deixar a estrutura **middleware-ready** para que adicionar auth seja trivial.

**Justificativa.** O case não exige autenticação. Implementar auth real (JWT/OAuth/IAM) é overhead grande e fora de escopo. Mas estruturar mal o handler hoje significa que adicionar middleware depois vira refactor.

**Implementação.**
```go
// services/aggregator/internal/infra/api/router.go
func NewRouter(h *Handler, mws ...func(http.Handler) http.Handler) http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /health", h.Health)
    mux.HandleFunc("GET /metrics/{developer_id}", h.ListEvents)
    mux.HandleFunc("GET /metrics/{developer_id}/summary", h.Summary)

    var handler http.Handler = mux
    for i := len(mws) - 1; i >= 0; i-- {
        handler = mws[i](handler)
    }
    return handler
}
```

Em `cmd/aggregator/main.go`:
```go
router := api.NewRouter(handler /*, authMiddleware */)
```

**README terá nota explícita:**
> "API exposed on `:8080` without authentication for the demo. In production this would sit behind an API Gateway with IAM auth, or a service mesh with mTLS. The router accepts middlewares as a variadic so adding auth is a one-line change."

---

## Resumo das decisões

| ID | Decisão | Impacto na Fase 3 |
| --- | --- | --- |
| D1 | `schema_version: 1` na mensagem | Adicionar campo na struct ProcessedEvent (Processor) e validar no Aggregator |
| D2 | TTL de 30 dias na tabela `events` | Adicionar `expires_at` no record + comando no init-aws.sh |
| D3 | Aceitar hot partition, monitorar | Documentar no README; sem código adicional |
| D4 | Componentes da média, não média calculada | Schema do `developer_summary` com `total_review_time_minutes` + `review_time_count` |
| D5 | `TransactWriteItems` para atomicidade | Use case do Aggregator usa transação atômica |
| D6 | API sem auth, mas middleware-ready | Router aceita variadic de middlewares |

## Próximo passo

Abrir `feat/phase-3-aggregator` com:
1. Pequena mudança no Processor (D1: adicionar `schema_version`).
2. Aggregator completo seguindo Clean Architecture (mesmo padrão do Processor).
3. Tabela `events` com TTL configurado no init-aws.sh.
4. API REST com router middleware-ready.
5. Idempotência via `TransactWriteItems`.
6. Testes unitários: idempotência, agregação, handlers HTTP.

Critério de pronto da Fase 3: `make send-test` → mensagem aparece em `events`, `developer_summary` é incrementado, `GET /metrics/dev-1/summary` retorna o agregado correto.
