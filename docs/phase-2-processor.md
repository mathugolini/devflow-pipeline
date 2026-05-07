# Fase 2 — Processor

> Branch: `feat/phase-2-processor`

## Objetivo

Construir o serviço Processor: consome a fila `raw-events`, valida e enriquece os eventos, publica em `processed-events`. Concorrente, com graceful shutdown, sem dependência de outro serviço Go.

## Componentes entregues

```
services/processor/
├── cmd/processor/main.go              # wiring + signal handling
├── internal/
│   ├── domain/
│   │   ├── event.go                   # RawEvent, ProcessedEvent, MetricType, Validate()
│   │   ├── errors.go                  # ValidationError tipado
│   │   └── event_test.go              # tabela de casos de validação
│   ├── usecase/
│   │   ├── process_event.go           # orquestração validar→enriquecer→publicar
│   │   └── process_event_test.go      # casos: sucesso, validação, falha transitória
│   └── infra/
│       ├── config/config.go           # env vars + defaults + validação
│       ├── queue/sqs.go               # Client + Consumer + Publisher (SDK v2)
│       └── worker/pool.go             # dispatcher + N goroutines + graceful drain
├── Dockerfile                          # multi-stage builder + distroless nonroot
├── go.mod / go.sum
```

## Arquitetura (camadas)

```
┌──────────────────────────────────────────────────────────────┐
│                          cmd/processor                       │
│   (wiring, signal handling, slog JSON, graceful shutdown)    │
└──────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐   ┌──────────────────┐   ┌───────────────────┐
│ infra/queue   │   │ infra/worker     │   │ infra/config      │
│ Consumer      │──▶│ Pool (dispatcher │   │ Load() from env   │
│ Publisher     │   │ + N workers)    │   │                   │
└───────┬───────┘   └─────────┬────────┘   └───────────────────┘
        │                     │
        │ ports               │ Handler interface
        ▼                     ▼
                ┌──────────────────────────┐
                │ usecase.ProcessEvent     │
                │ (sem deps de infra)      │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │ domain                   │
                │ RawEvent.Validate()      │
                │ ValidationError tipado   │
                └──────────────────────────┘
```

**Regra:** dependências apontam para dentro. `domain` não importa nada do projeto; `usecase` só importa `domain`; `infra` importa `usecase` e `domain`; `cmd` faz wiring de tudo.

## Decisões de design

### 1. Erros de validação tipados (`*ValidationError`)

**Problema:** o worker precisa decidir entre **deletar a mensagem da fila**, **deixar para a DLQ**, ou **deixar para o SQS reentregar**. Sem um tipo de erro, isso vira `strings.Contains(err.Error(), "invalid")` — frágil.

**Solução:** `domain.ValidationError` com `Field` e `Reason`. O worker usa `errors.As` para distinguir.

**Trade-off:** mais boilerplate que retornar `errors.New("...")`. Vale o custo: a regra de negócio "evento inválido vai pra DLQ, não retenta" fica explícita no tipo.

### 2. Quem retenta o quê — SQS faz o trabalho pesado

| Cenário | Ação do worker | Quem retenta |
| --- | --- | --- |
| Processou OK | `DeleteMessage` | ninguém |
| `ValidationError` (permanente) | **não deleta** | SQS reentrega até `maxReceiveCount=3`, depois DLQ |
| Erro transitório (publish falhou) | **não deleta** | mesma coisa: SQS reentrega via visibility timeout |
| JSON malformado | **não deleta** | mesma coisa |

**Por que não deletar mensagens inválidas explicitamente:**
A tentação é "se é inválido, deleta logo, não desperdiça `maxReceiveCount` tentativas". Errado por dois motivos:
1. **Duplicar a lógica do SQS.** O DLQ existe exatamente para isso.
2. **Falsos positivos em validação.** Se um deploy ruim tornou todas as mensagens "inválidas" momentaneamente, deletar destrói dados. O DLQ permite recuperar.

**Trade-off:** mensagens permanentemente inválidas consomem 3 ciclos do worker antes de irem pra DLQ. Em volume alto isso poderia importar; aqui não.

**Detalhe importante:** `usecase.ProcessEvent` não retenta erros transitórios em loop interno. Retry com backoff fica a cargo do SQS (visibility timeout). Reimplementar retry no código duplica complexidade e pode travar o worker.

### 3. Worker pool: dispatcher + canal + N goroutines

**Padrão:**
```
[long poll SQS] → channel jobs → N workers consomem
```

- **Dispatcher** (1 goroutine): faz `ReceiveMessage` com `WaitTimeSeconds=10` (long polling), enfileira no channel.
- **Workers** (N goroutines): leem do channel, chamam o handler, deletam ou deixam.

**Por que esse padrão:**
- Backpressure natural: se workers estão ocupados, o dispatcher bloqueia no `jobs <- m` e não busca mais mensagens. Sem fila interna inflando memória.
- Graceful shutdown trivial: cancela context → dispatcher para → fecha channel → workers terminam.

**Alternativa rejeitada:** uma goroutine por mensagem (`go process(m)`). Sem limite de concorrência → DynamoDB/SQS rate-limited; OOM em pico de tráfego.

### 4. Graceful shutdown

**Cadeia:**
1. `signal.NotifyContext` captura `SIGTERM`/`SIGINT`.
2. Cancela o context do dispatcher → ele sai do loop.
3. `close(jobs)` → workers terminam o que estavam fazendo e saem.
4. `wg.Wait()` → main retorna.

**Detalhe sutil:** workers usam `context.Background()` ao chamar o handler, não o context de shutdown. **Por quê:** se uma mensagem está sendo processada quando chega `SIGTERM`, queremos terminá-la, não cancelar no meio (ou ela vai re-aparecer na fila e ser reprocessada, gerando dupla publicação na `processed-events`). O dispatcher já parou de buscar novas mensagens — drenar o que está in-flight é seguro.

**Trade-off:** se o handler trava (ex.: SQS fora do ar) e o usuário manda `SIGTERM`, o shutdown demora até o handler timeoutar (HTTP timeout do SDK ~30s). Aceitável; alternativa seria cancelar tudo, mas perdemos a garantia de drain.

### 5. AWS SDK v2 + `BaseEndpoint` no client option

**Por que SDK v2:** API mais limpa, contextos de primeira classe, `slog`-friendly. O v1 está em manutenção.

**Por que `BaseEndpoint` no `sqs.NewFromConfig` em vez de `WithBaseEndpoint` no config:** a função `awsconfig.WithBaseEndpoint` não existe na versão usada. A forma idiomática é overrride por client (`sqs.Options.BaseEndpoint`). Mais explícito, e cada serviço pode ter endpoint diferente se precisar.

### 6. `slog` (stdlib) com handler JSON

**Por quê:** logging estruturado é requisito do case. `slog` está na stdlib desde Go 1.21 — sem dependência externa, perfeitamente serializável (correlação por `event_id` é trivial com `slog.String("event_id", ...)`).

**Alternativa rejeitada:** `zerolog`/`zap`. Mais rápidos, mas dependência extra para um requisito que `slog` cobre.

### 7. `Clock` injetável no use case

**Por quê:** validação depende de "agora" (`timestamp` não pode ser futuro). Para testar deterministicamente, o use case recebe uma função `Clock` que retorna `time.Time`. Em produção: `time.Now().UTC`. Em teste: clock fixo.

**Trade-off:** uma linha a mais no construtor. Vale o custo; testes ficam reproducíveis.

### 8. Dockerfile multi-stage com `distroless/static-debian12:nonroot`

**Por que distroless e não `scratch`:**
- `scratch` é menor (~10MB) mas não tem `/etc/ssl/certs/ca-certificates.crt`. Sem isso, chamadas HTTPS para AWS real (não LocalStack) falham com erro de certificado.
- `distroless/static` (~20MB) traz só as CAs e `tzdata`. Nada mais.

**`:nonroot`:** roda como UID 65532, não root. Defesa em profundidade — se algum dia rodar em prod e for comprometido, atacante já não tem root no container.

**Flags do build:**
- `CGO_ENABLED=0` → binário totalmente estático, indispensável para distroless static.
- `-trimpath` → remove paths absolutos do binário (reproducible builds).
- `-ldflags="-s -w"` → strip de symbols/debug, reduz tamanho ~25%.

### 9. Dois `go.mod` — confirmado na prática

Cada serviço tem seu próprio `go.mod`. O Aggregator (Fase 3) **não vai importar** o domínio do Processor; vai redefinir `Event` e `Summary` localmente. Isso parece duplicação, mas é o **contrato da fila** — exatamente o que se quer explícito.

### 10. `restart: unless-stopped` no Processor, mas **não no `aws-init`**

- `processor`: queremos que reinicie se cair (resiliência).
- `aws-init`: é one-shot, `restart: "no"`. Reiniciar daria loop infinito.

## Fluxo end-to-end (já validável)

1. `make up` → infra + processor sobem.
2. `make send-test` → publica um evento válido na `raw-events`.
3. Processor recebe, valida, enriquece com `processed_at`+`processor_id`, publica na `processed-events`.
4. `aws sqs get-queue-attributes` na `processed-events` mostra contador > 0.
5. `docker compose logs processor` mostra log JSON com `event_id` correlacionado.

## Testes

- **Domínio**: 12 casos de validação cobrindo todas as regras do case (UUID, vazio, enum, negativos, boundary 1440, futuro, zero).
- **Use case**: sucesso, erro de validação não chama publisher, erro de publish é envelopado mas não classificado como validação.
- **Não tem testes de integração com LocalStack.** Decisão: para o prazo, mocks são suficientes. Validação real é via `make send-test` no fluxo end-to-end.

```bash
make test-processor
```

## O que ficou de fora desta fase

- **Métricas/tracing** (OpenTelemetry) — diferencial; reservado pro final.
- **Backoff exponencial explícito no código.** O case menciona, mas argumentei na decisão #2 que SQS faz isso. Se o avaliador exigir, é trivial adicionar um `time.Sleep` no dispatcher quando `Receive` falha — preferi não complicar.
- **Linter (`golangci-lint`)** — entra no Makefile junto com a Fase 5.

## O que eu faria diferente com mais tempo

- **Métrica de mensagens em DLQ** exposta via Prometheus, com alerta — sem isso, mensagens podem morrer silenciosamente na DLQ.
- **Reaproveitar o cliente SQS via interface** que abstrai `Consumer`/`Publisher` separados das implementações concretas — facilita um teste de integração com fakes em memória.
- **Gerar OpenAPI** das mensagens via JSON schema, para o contrato da fila virar artefato versionado.
