# Revisão pré-Fase 3 — itens residuais e decisões cross-cutting

> Segunda passada após o hardening do PR #5. Foco em (a) o que **ficou** no projeto e (b) o que precisa ser **decidido agora** antes de o Aggregator cimentar contratos.

---

## 🔴 Itens novos (não estavam na primeira revisão)

### N1. Worker pool sem `recover()` — uma única `panic` deadlock o pool

**Risco.** Se `handler.Execute` der `panic` (nil pointer dereference, divide-by-zero, type assertion errada — qualquer regressão futura), a goroutine do worker morre **sem** rodar `wg.Done()`. O pool degrada silenciosamente para N-1 workers. Repete N vezes e o pool fica com 0 workers — `wg.Wait()` nunca retorna, shutdown trava (mesmo com o A4 — o `shutdownGrace` salva o processo, mas mensagens ficam paradas até lá).

**Severidade.** Alto. É o tipo de bug que aparece 6 meses depois com uma mensagem específica, em produção, e ninguém entende por que o pipeline parou.

**Localização.** `services/processor/internal/infra/worker/pool.go:workerLoop`.

**Correção.**

```go
func (p *Pool) workerLoop(id int, jobs <-chan queue.Message) {
    for msg := range jobs {
        p.runOne(id, msg)
    }
}

func (p *Pool) runOne(id int, msg queue.Message) {
    defer func() {
        if r := recover(); r != nil {
            p.log.Error("worker panic recovered",
                slog.Any("panic", r),
                slog.String("event_id", msg.Event.EventID),
                slog.String("stack", string(debug.Stack())),
            )
            // Don't delete — let SQS redrive to DLQ.
        }
    }()
    hctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
    defer cancel()
    p.handle(hctx, id, msg)
}
```

Nota: o `defer recover()` precisa estar em uma função que **retorna**, daí o `runOne` dedicado.

### N2. JSON aceita campos desconhecidos silenciosamente — schema drift invisível

**Risco.** `json.Unmarshal` ignora campos extras por default. Se um produtor começar a enviar `event_version: 2` com novo formato, o Processor processa o que reconhece e descarta o resto. Bugs sutis: campo renomeado de `developer_id` para `dev_id` → todas as mensagens passam pela validação como `developer_id` vazio (rejeitadas em massa, mas o erro reportado é misleading).

**Severidade.** Alto pra um sistema que tem dois serviços conversando por contrato JSON sem schema validator no meio.

**Localização.** `services/processor/internal/infra/queue/sqs.go:Receive`.

**Correção.**

```go
dec := json.NewDecoder(bytes.NewReader([]byte(*m.Body)))
dec.DisallowUnknownFields()
var ev domain.RawEvent
if err := dec.Decode(&ev); err != nil {
    parseFailures = append(parseFailures, m)
    continue
}
```

**Trade-off.** Strict mode quebra mensagens que produtores adicionarem campo novo "inocente". Mitiga: combinar com **schema versioning** (item D1 mais abaixo) — campo extra = nova versão = release coordenado.

### N3. `time.After` em loop pode vazar timers até disparar

**Risco.** No backoff do `dispatch`, `time.After(d)` aloca um timer que **só** é coletado quando dispara. Se o ctx cancela no meio do `select`, o timer fica vivo até `d` segundos. Em backoff de 30s + alta rotatividade de instâncias (ex.: deploy rolling), isso vira lixo de heap mensurável.

**Severidade.** Baixo na prática, mas é o tipo de coisa que reviewer experiente comenta — sinaliza atenção a recursos.

**Localização.** `services/processor/internal/infra/worker/pool.go:dispatch`.

**Correção.**

```go
t := time.NewTimer(backoff + jitter(backoff))
select {
case <-ctx.Done():
    if !t.Stop() {
        <-t.C
    }
    return
case <-t.C:
}
```

---

## 🟡 Decisões cross-cutting — fechar **agora** ou pagar caro depois

Estes não são bugs, são **escolhas de design** que precisam ser feitas conscientemente antes do Aggregator cimentar o contrato. Cada uma vai virar uma seção do `docs/phase-3-aggregator.md`.

### D1. Schema versioning na mensagem da fila

**Decisão a tomar.** Adicionar `schema_version: 1` na `ProcessedEvent` agora, ou viver sem.

**Por que importa.** O Aggregator vai começar a persistir esses eventos no DynamoDB. Sem versionamento, mudar o formato no futuro exige (a) deploy coordenado dos dois serviços ou (b) lógica condicional baseada em campos opcionais — código sujo crônico.

**Recomendação.** Adicionar agora. Custo: 1 linha em cada struct + 1 teste. Recompensa enorme em 6 meses.

```go
type ProcessedEvent struct {
    SchemaVersion int `json:"schema_version"` // = 1
    RawEvent
    ProcessedAt time.Time `json:"processed_at"`
    ProcessorID string    `json:"processor_id"`
}
```

E no Aggregator: rejeitar versões desconhecidas (envia pra DLQ). Compatibilidade forward via "if v == 2 { ... }" quando chegar.

### D2. Idempotência: janela de tempo ou eterna?

**Decisão a tomar.** O Aggregator vai usar `ConditionExpression: attribute_not_exists(event_id)` no DynamoDB para deduplicar. Isso garante idempotência **eterna** (enquanto a row existir). Mas isso significa que `events` cresce monotonicamente sem TTL → custo crescente, scans cada vez mais caros.

**Alternativas.**

1. **Tabela `events` sem TTL** — idempotência perfeita, custo crescente. Bom pra audit trail.
2. **Tabela `events` com TTL de 30 dias** — idempotência efetiva (SQS dedup window é 5 min, retry window é minutos). Custo controlado. Bom default.
3. **Não persistir cada evento, só o summary** — perde audit trail. Ruim pra debugging.

**Recomendação.** Opção 2: TTL de 30 dias na tabela `events` via atributo `expires_at`. Cobre 100% dos cenários de retry/redelivery. Documentado claramente: "se quiser retroceder além de 30 dias, vai pra cold storage (S3) via DynamoDB Streams".

```go
type EventRecord struct {
    EventID    string    `dynamodbav:"event_id"`
    ExpiresAt  int64     `dynamodbav:"expires_at"` // Unix epoch
    // ...
}
```

E na criação da tabela: `aws dynamodb update-time-to-live --time-to-live-specification "Enabled=true,AttributeName=expires_at"`.

### D3. Hot partition no `developer_summary`

**Risco.** `developer_id` como partition key. Se um dev é "anormalmente ativo" (bot? CI service account?), todas as escrituras vão pra mesma partição → throttling do DynamoDB.

**Mitigação.** Algumas opções:

1. **Aceitar e monitorar.** Em prod, a maioria dos times tem distribuição razoável. CloudWatch alarme em `ConsumedWriteCapacityUnits` por partição.
2. **Sharding artificial.** `developer_id#shard0..N`, agregar na leitura. Complexa, troca write hotspot por read fanout.
3. **Buffer + flush periódico.** Em vez de escrever a cada evento, agregar na memória do Aggregator e flush a cada 5s. Reduz QPS por dev em 50-100x. **Compromete idempotência simples** — fica complexo recuperar de crash.

**Recomendação.** Opção 1 com nota explícita no doc. PAY_PER_REQUEST mitiga o problema de throttle (DynamoDB redistribui automaticamente). Em provisioned mode, opção 3 vale a pena.

### D4. `avg_review_time_minutes` — armazenar média ou componentes?

**Risco já identificado em conversas anteriores.** Vale repetir explicitamente: armazenar a média **calculada** (`avg = total/count`) sofre de drift de ponto flutuante e impossibilita correções. Armazenar `total_review_time_minutes` + `review_time_count` separadamente, calcular média na leitura.

**Decisão.** Schema da tabela `developer_summary`:

```
{
  developer_id:                 "dev-123",     // PK
  total_commits:                142,           // ADD para incremento atômico
  total_pull_requests:          38,
  total_review_time_minutes:    9023.5,
  review_time_count:            199,
  events_processed:             195,
  last_activity:                "2026-04-15T10:30:00Z",
}
```

API REST calcula `avg_review_time_minutes = total_review_time_minutes / review_time_count` na resposta (com guarda contra divisão por zero).

### D5. O que faz o Aggregator quando `processed-events` envia evento já agregado mas summary update falha?

**Cenário.** `events.PutItem` com conditional write succeeds (evento novo). `developer_summary.UpdateItem` falha (rede, throttle). E agora?

**Opções.**

1. **Retornar erro do handler.** SQS redrive. Mas na próxima tentativa, `events.PutItem` falha com `ConditionalCheckFailedException` (já existe), e Aggregator pode interpretar como "já processado" → summary nunca atualizado. Bug silencioso.
2. **Idempotência no summary update.** Manter um set de `processed_event_ids` no row do summary, checar antes de incrementar. Caro em payload.
3. **Atomicidade via DynamoDB Transactions.** `TransactWriteItems` cobrindo as duas escritas. Atômico de verdade. Custo: 2x WCU.

**Recomendação.** Opção 3. `TransactWriteItems` é exatamente pra isso. Custo extra (2x WCU em vez de 1x) é aceitável pra eliminar uma classe inteira de bug.

```go
client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
    TransactItems: []types.TransactWriteItem{
        {Put: &types.Put{
            TableName: aws.String("events"),
            Item: eventItem,
            ConditionExpression: aws.String("attribute_not_exists(event_id)"),
        }},
        {Update: &types.Update{
            TableName: aws.String("developer_summary"),
            // ADD/SET expressions
        }},
    },
})
```

Se o evento já existe (`ConditionalCheckFailedException`), a transação inteira é rollback — nem `events` nem `summary` mudam. Idempotência garantida.

### D6. API REST sem autenticação

**Decisão consciente do case** ou ponto a documentar?

**Risco.** A API expõe métricas de produtividade individuais. PII real. Em qualquer ambiente compartilhado, sem auth = leak.

**Recomendação.** Documentar no README:
> "API exposed on `:8080` without authentication for the demo. In production this would sit behind an API Gateway with IAM auth, or a service mesh with mTLS. The handler layer is intentionally stateless to make adding middleware trivial."

E **deixar o handler preparado** pra middleware:

```go
func authMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // skipped in demo
        next.ServeHTTP(w, r)
    })
}

router.Use(authMiddleware)
```

---

## 🟢 Itens menores que ainda valem (resumo do que não foi feito)

Da revisão anterior, **continuam pendentes** (severidade média/baixa):

| ID | Item | Quando atacar |
| --- | --- | --- |
| M2 | `value` aceita float pra `commits`/`pull_requests` | Fase 5 polimento |
| M3 | Sem teto em `value` | Fase 5 |
| M4 | Sem rule "evento muito antigo" | Fase 5 |
| M6 | Image tags mutáveis | Antes de qualquer mention de "produção-ready" |
| M7 | Creds AWS no compose com nota explícita | Junto com M6 |
| M8 | `govulncheck`/`trivy` no CI | Fase 5 + CI |
| M9 | Resource limits no compose | Fase 5 |
| B3 | `/healthz` no Processor | Fase 5 quando Aggregator também tiver `/health` |
| B4 | Métricas Prometheus | Diferencial — só se sobrar tempo |

---

## Recomendação para a Fase 3

**Antes de abrir o branch `feat/phase-3-aggregator`:**

1. **Aplicar N1, N2, N3** num PR pequeno (`fix/processor-resilience`). Esses três blindam o Processor contra panics, schema drift e timer leaks. ~30 min de trabalho.

2. **Tomar as decisões D1-D6 e documentar** em `docs/phase-3-decisions.md`. Não escrever código ainda — só fechar as escolhas. Vai economizar refactor depois.

3. **Aí sim, começar a Fase 3** com o contrato cimentado: `ProcessedEvent` com `schema_version`, schema do DynamoDB com componentes da média, transação atômica para escrita.

**Se quiser priorizar:**
- N1 (panic recovery) é o único item do qual eu **não avançaria sem**.
- N2 e N3 são "muito convenientes ter".
- D1, D2, D5 mudam o desenho do Aggregator se decididas tarde — fechar antes.
- D3, D4, D6 dão pra documentar e não bloqueiam o código.

Manda como você prefere proceder.
