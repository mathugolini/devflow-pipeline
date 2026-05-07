# Revisão de segurança e qualidade — Fases 1 e 2

> Inventário do que um revisor sênior provavelmente questionaria antes de aprovar isso pra produção. Cada item: **risco**, **impacto**, **localização**, **correção proposta**.

Severidades:
- 🔴 **Alto** — bug latente, vetor real de DoS, ou problema operacional grave
- 🟡 **Médio** — questionável em produção; fácil de explorar ou degradar
- 🟢 **Baixo** — polimento, hardening defensivo, "smart reviewer would notice"

---

## 🔴 Alto

### A1. Sem idempotência no Processor → duplicação na `processed-events`

**Risco.** SQS Standard é **at-least-once**. A mesma mensagem pode ser entregue 2× ao Processor. Hoje, isso publica 2× na `processed-events` — o Aggregator depois deduplica via `event_id`, mas o ônus está no destino e a fila intermediária infla.

**Localização.** `services/processor/internal/usecase/process_event.go` — `Execute` não checa duplicatas.

**Correção.**
- **Curto prazo (escolha consciente, já documentada):** confiar no Aggregator pra deduplicar e aceitar o desperdício.
- **Médio prazo:** trocar a `processed-events` por **FIFO queue** + `MessageDeduplicationId = event_id`. SQS dedupe nativo por 5 min.
- **Longo prazo:** publicar com `MessageDeduplicationId` e `MessageGroupId = developer_id` — preserva ordenação por dev e dedup automático.

### A2. `VisibilityTimeout` fixo de 30s pode causar reprocessamento indevido

**Risco.** Se `Publish` na próxima fila demora >30s (LocalStack lento, rate limiting da AWS, GC pause), a mensagem volta a ser visível, **outro worker pega, e duplica**.

**Localização.** `infra/localstack/init-aws.sh`, `VisibilityTimeout: "30"`.

**Correção.**
- Implementar **heartbeat de visibility extension**: enquanto o handler trabalha, uma goroutine paralela chama `ChangeMessageVisibility` a cada 20s. Cancelada quando o handler retorna.
- Ou pelo menos: aumentar para 60s e medir p99 real do `Publish`.

```go
func extendVisibility(ctx context.Context, c *sqs.Client, url, handle string) {
    t := time.NewTicker(20 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-t.C:
            _, _ = c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
                QueueUrl: &url, ReceiptHandle: &handle, VisibilityTimeout: 30,
            })
        }
    }
}
```

### A3. `Receive` em hot loop quando o backend está fora

**Risco.** `dispatch()` faz `continue` em qualquer erro do `Receive`. Se LocalStack/SQS está fora, o loop estoura logs em milhões/segundo e queima CPU/quota.

**Localização.** `services/processor/internal/infra/worker/pool.go`, função `dispatch`.

**Correção.** Backoff exponencial com jitter quando `Receive` falha:

```go
backoff := time.Second
for {
    msgs, _, err := p.consumer.Receive(ctx)
    if err != nil {
        p.log.Error("receive failed", slog.Any("err", err))
        select {
        case <-ctx.Done(): return
        case <-time.After(backoff):
        }
        backoff = min(backoff*2, 30*time.Second) + jitter()
        continue
    }
    backoff = time.Second // reset
    // ...
}
```

### A4. Graceful shutdown pode travar indefinidamente

**Risco.** Workers chamam o handler com `context.Background()` (escolha intencional pra não duplicar). Se o handler trava (SDK retry sem timeout, deadlock, etc.), `wg.Wait()` nunca retorna. Nenhum supervisor (compose, k8s) pode encerrar limpo — só `SIGKILL`.

**Localização.** `services/processor/internal/infra/worker/pool.go`, `workerLoop` → `handle(context.Background(), ...)`.

**Correção.** Context detached **mas com timeout duro**:

```go
hctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
defer cancel()
p.handle(hctx, id, msg)
```

E no `main`, depois de `pool.Run(ctx)`:

```go
done := make(chan struct{})
go func() { pool.Run(ctx); close(done) }()
<-ctx.Done()
select {
case <-done:
case <-time.After(30 * time.Second):
    log.Error("hard shutdown after grace period")
    os.Exit(1)
}
```

### A5. `LOG_LEVEL` é lido da config mas ignorado no `main`

**Risco.** `config.Load` retorna `LogLevel`, mas `main.go` hardcoda `slog.LevelInfo`. Operador troca env var, nada muda. Em produção isso vira ticket de "logs não rotacionam direito".

**Localização.** `services/processor/cmd/processor/main.go:11-13`.

**Correção.**

```go
var lvl slog.Level
if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
    lvl = slog.LevelInfo
}
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
```

(precisa carregar a config antes do logger, ou logar inicial em info e trocar handler depois.)

### A6. `developer_id` e `repository` sem limite de tamanho

**Risco.** SQS aceita até 256KB por mensagem. Um `developer_id` de 200KB passa pela validação atual (não-vazio). Atacante pode encher DynamoDB, inflar logs, custar dinheiro.

**Localização.** `services/processor/internal/domain/event.go`, função `Validate`.

**Correção.**

```go
const (
    maxDeveloperIDLen = 128
    maxRepositoryLen  = 256
)
if len(e.DeveloperID) > maxDeveloperIDLen {
    return newValidationError("developer_id", "exceeds 128 chars")
}
if len(e.Repository) > maxRepositoryLen {
    return newValidationError("repository", "exceeds 256 chars")
}
```

Bonus: rejeitar caracteres de controle (`\x00-\x1F`, exceto `\t\n\r`) — defesa contra log injection e UI rendering issues.

---

## 🟡 Médio

### M1. `PROCESSOR_ID` default não é único entre réplicas

**Risco.** Se você sobe 3 réplicas com a env padrão, todas se identificam como `processor-1`. Telemetria fica inútil pra debug ("qual réplica processou esse evento?").

**Localização.** `internal/infra/config/config.go`, default `"processor-1"`.

**Correção.**

```go
import "os"
host, _ := os.Hostname()
ProcessorID: getenv("PROCESSOR_ID", host),
```

Em compose/k8s, hostname já é único por container/pod.

### M2. `value` aceita `float64` para `commits` e `pull_requests`

**Risco.** "2.5 commits" passa pela validação. Aggregator agrega float corretamente, mas semanticamente é nonsense — e em prod alguém vai usar isso como mostrar "2.5 commits hoje".

**Correção.** Validar que `commits` e `pull_requests` são inteiros:

```go
if (e.MetricType == MetricCommits || e.MetricType == MetricPullRequests) &&
    e.Value != math.Trunc(e.Value) {
    return newValidationError("value", "must be an integer for commits/pull_requests")
}
```

### M3. Sem limite máximo em `value`

**Risco.** `value: 1e308` (próximo ao `math.MaxFloat64`) passa. Quando o Aggregator somar, vira `+Inf`, corrompendo o summary.

**Correção.** Limite por tipo:

```go
const (
    maxCommitsPerEvent      = 10_000  // 1 evento ≠ 10k commits
    maxPullRequestsPerEvent = 1_000
)
```

### M4. Timestamp não tem "muito antigo"

**Risco.** Um evento de 2010 é "válido" hoje. Backfill malicioso ou bug de cliente pode poluir agregações históricas.

**Correção.**

```go
const maxEventAge = 24 * time.Hour
if now.Sub(e.Timestamp) > maxEventAge {
    return newValidationError("timestamp", "event is too old (>24h)")
}
```

### M5. `Ping` no boot usa `ListQueues`

**Risco.** Passa mesmo se as filas específicas (`raw-events`, `processed-events`) ainda não existem. O `compose depends_on` mitiga, mas o assert é fraco — em ambientes dinâmicos (k8s, prod), isso falha tarde.

**Localização.** `internal/infra/queue/sqs.go`, `Ping()`.

**Correção.** Já chamamos `GetQueueUrl` logo depois — basta remover o `Ping` ou trocar por `GetQueueUrl` da fila real.

### M6. Image tags mutáveis no Dockerfile e compose

**Risco.** `golang:1.22-alpine`, `localstack/localstack:3.8`, `gcr.io/distroless/static-debian12:nonroot`, `amazon/aws-cli:2.17.0` são tags mutáveis. Mantenedores podem republicar a tag com payload diferente — supply chain attack vector real.

**Correção.** Pinar por digest:

```dockerfile
FROM golang:1.22-alpine@sha256:1699c10032ca2... AS builder
FROM gcr.io/distroless/static-debian12@sha256:a9329520abc4...
```

Renovar via Renovate/Dependabot.

### M7. Credenciais AWS hardcoded no compose

**Risco.** `AWS_ACCESS_KEY_ID=test` é OK pra LocalStack. **Mas** o `docker-compose.yml` é frequentemente o template do `docker-compose.prod.yml`. É comum esse `test/test` viajar pra prod por copy-paste.

**Correção.** Mover pra `.env` (já listado no `.gitignore`) e usar interpolação no compose:

```yaml
environment:
  - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:?required}
  - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:?required}
```

Adicionar comentário ostensivo: `# LOCALSTACK ONLY — replace via secrets in any non-local env`.

### M8. Sem vulnerability scanning de deps

**Risco.** AWS SDK e suas transitivas têm CVEs reais e regulares. Não há gate em CI.

**Correção.** `govulncheck` no Makefile + CI:

```bash
govulncheck ./...
```

Bonus: `trivy image devflow-pipeline-processor:latest` no CI.

### M9. Sem resource limits nos containers

**Risco.** Processor com vazamento de memória pode consumir todo RAM da máquina e derrubar LocalStack junto. No compose, sem limites.

**Correção.**

```yaml
processor:
  deploy:
    resources:
      limits:
        memory: 256M
        cpus: "1.0"
```

### M10. `restart: unless-stopped` sem backoff explícito

**Risco.** Bug que crasha no startup → loop infinito sem visibilidade. Compose não tem CrashLoopBackoff equivalente.

**Correção.** Adicionar `restart_policy` com `max_attempts` (válido em Swarm) ou implementar healthcheck no Processor que cause failed state.

---

## 🟢 Baixo

### B1. `uuid.Parse` é permissivo demais

**Risco.** Aceita formatos não-canônicos (`{xxx}`, `urn:uuid:xxx`, sem hífens). Se o contrato é "UUID v4 36 chars", reviewer estrito vai questionar.

**Correção.** `uuid.Validate(s)` (disponível em `google/uuid` v1.6.0+) é estrito.

### B2. `slog.String("err", err.Error())` perde a chain de wrapping

**Risco.** Se um erro foi wrapado com `fmt.Errorf("publish: %w", inner)`, logar só `Error()` perde estrutura. Uma extração programática (filtro por causa raiz no log search) fica difícil.

**Correção.** `slog.Any("err", err)` — o handler JSON serializa o erro, e se for tipado, dá pra anotar com mais campos.

### B3. Healthcheck do Processor ausente

**Risco.** Compose só sabe que o container está "rodando", não que está saudável. K8s liveness/readiness probe vai querer um endpoint.

**Correção.** Adicionar um listener HTTP simples no Processor:

```go
go http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if err := client.Ping(r.Context()); err != nil {
        http.Error(w, err.Error(), 503); return
    }
    w.WriteHeader(200)
}))
```

E:

```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:8080/healthz"]
```

### B4. Sem métricas

**Risco.** Mensagens podem morrer silenciosamente na DLQ; backlog na fila pode crescer; latência p99 do `Publish` pode subir. Sem métricas, descobrimos via reclamação.

**Correção.** `prometheus/client_golang` expondo:
- `processor_messages_total{status="ok|validation_error|transient_error"}`
- `processor_publish_duration_seconds`
- `processor_inflight`

Bonus: scraper que lê `ApproximateNumberOfMessages` da DLQ a cada 30s e expõe.

### B5. Falha de parse JSON é loggada toda iteração

**Risco.** Mensagem mal-formada permanece visível por 30s, é re-recebida 3 vezes antes de ir pra DLQ. Cada vez gera um `WARN`. Em volume, polui logs.

**Localização.** `pool.go`, loop `for _, m := range parseFails`.

**Correção.** Logar com nível `Debug` ou usar deduplicação por `MessageId` (cache em memória, TTL = visibility timeout).

### B6. Dispatcher loga em Error e continua sem distinguir tipos de erro do SDK

**Risco.** Erros transitórios (timeout, throttle) e permanentes (queue não existe, credencial inválida) viram a mesma linha de log. Operador precisa entender o stack trace.

**Correção.** Match nos tipos do `aws-sdk-go-v2` (`*types.QueueDoesNotExist`, `*smithy.OperationError`) e tratar diferente: throttle → backoff agressivo; queue not found → exit fatal.

### B7. Sem `pprof` debug endpoint

**Risco.** Em produção, sem `pprof`, debugar uma goroutine leak ou um lock contention precisa de um deploy novo.

**Correção.** Sob env `PPROF_ADDR`, expor `net/http/pprof` em uma porta separada da healthcheck:

```go
if addr := os.Getenv("PPROF_ADDR"); addr != "" {
    go http.ListenAndServe(addr, nil)
}
```

### B8. `Worker` interno do worker pool não respeita o ctx parent na chamada do handler

**Risco.** Já discutido em A4. Se a opção for "handler recebe ctx detached", o ctx **deve** ter timeout próprio. Nunca um detached sem deadline.

### B9. `config.Load` retorna `Config` por valor mesmo em erro

**Risco.** Caller ingênuo ignora erro e usa Config zerada → falha tarde. Pequeno, mas reviewer estrito vai apontar.

**Correção.** Retornar `(*Config, error)` e nil em erro.

### B10. `make send-test` espera 3 segundos hardcoded

**Risco.** Em máquinas lentas, 3s pode não ser suficiente — teste falha aparentemente, mas mensagem foi processada 2s depois. Fricção pra demonstração.

**Correção.** Polling até `ApproximateNumberOfMessages > 0` com timeout de 15s.

---

## Itens que **não** vou abordar (com justificativa)

- **Criptografia de mensagens em trânsito (KMS).** Vale em prod; LocalStack não suporta nativamente, e o case explicitamente roda local.
- **VPC/Security Groups.** Fora do escopo de uma demo dockerizada.
- **IAM com least-privilege.** LocalStack ignora IAM por default. Em prod, o Processor só precisaria de `sqs:ReceiveMessage`/`DeleteMessage` na `raw-events` e `sqs:SendMessage` na `processed-events`.
- **Multi-tenancy em DynamoDB.** Aggregator (Fase 3) — mencionar lá.

---

## Top 5 que eu corrigiria **antes** da Fase 3

Se eu tivesse 1 hora pra hardenar antes de avançar:

1. **A4** — graceful shutdown com timeout duro (15 min)
2. **A3** — backoff exponencial no `Receive` (15 min)
3. **A5** — fixar `LOG_LEVEL` (10 min)
4. **A6** — limites de tamanho em strings (10 min)
5. **M1** — `PROCESSOR_ID` default = hostname (5 min)

Os outros entram na Fase 5 (polimento) ou ficam documentados como follow-up no README.

---

## Como apresentar isso no vídeo (opcional)

Se sobrar tempo no bloco "o que faria diferente":
> "Fiz uma autocrítica de segurança. Identifiquei 5 pontos altos: ausência de idempotência ponta a ponta, visibility timeout estático, hot loop em falha de receive, shutdown sem timeout duro, e LOG_LEVEL ignorado no boot. Está tudo documentado em `docs/security-and-review.md` com correção proposta. Os 3 primeiros eu corrigiria antes de qualquer deploy real."

Mostra autocrítica madura — o que normalmente diferencia um pleno de um sênior.
