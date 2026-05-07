# Roteiro do vídeo — devflow-pipeline (10 min)

> Use isto como guia. Fale com naturalidade, não leia. As falas são curtas de propósito.

## Estrutura

| Bloco | Tempo | O que mostrar |
| --- | --- | --- |
| 0. Abertura | 0:30 | Quem você é, o desafio em uma frase |
| 1. Arquitetura | 2:00 | Diagrama + 3 decisões que importam |
| 2. Como rodar | 1:00 | Pré-requisitos + comandos |
| 3. Demo ao vivo | 4:30 | Pipeline funcionando, validação, DLQ |
| 4. O que faria diferente | 1:30 | Honestidade técnica |
| 5. Fechamento | 0:30 | Convite pra revisão |

---

## 0. Abertura (0:30)

**Mostre:** o repositório no GitHub.

> "Oi, sou o Matheus. Esse é o meu case técnico para a vaga de Pleno Go no time de AI Coding Tools. Construí um pipeline de métricas de desenvolvedores: dois serviços Go conversando via SQS, com DynamoDB e API REST, tudo rodando em LocalStack via `docker-compose up`. Vou começar pela arquitetura, depois rodo ao vivo, e fecho com o que faria diferente."

---

## 1. Arquitetura (2:00)

**Mostre:** o diagrama do README e a estrutura de pastas (`tree -L 3`).

### O fluxo (15s)
> "O fluxo é clássico de event-driven: mensagem entra na `raw-events`, o **Processor** valida e enriquece, publica na `processed-events`, o **Aggregator** consome, faz idempotência, persiste no DynamoDB e expõe a API REST."

### Decisão 1 — Por que dois serviços, e não um monolito (30s)
**Mostre:** as duas pastas `services/processor` e `services/aggregator`, cada uma com seu `go.mod`.

> "Poderia ser um serviço só com duas goroutines. Mas separei por dois motivos: **escalabilidade independente** — o Aggregator é I/O-bound em DynamoDB, o Processor é CPU-bound em validação, têm perfis de carga diferentes; e **falha isolada** — se o Aggregator cai, mensagens enriquecidas ficam seguras na fila até ele voltar, o Processor segue trabalhando. Cada serviço tem seu próprio `go.mod` — isso força o contrato entre eles a ser **só o JSON na fila**, sem acoplamento por pacote Go."

### Decisão 2 — Clean Architecture com erros tipados (45s)
**Mostre:** `services/processor/internal/` — pastas `domain`, `usecase`, `infra`.

> "Cada serviço segue Clean Architecture: `domain` é puro Go, sem dependência externa. `usecase` orquestra. `infra` adapta SQS, DynamoDB, HTTP. Dependências apontam **para dentro**."

> "A decisão mais importante foi **erros tipados**. O `ValidationError` do domínio carrega `Field` e `Reason`. Por quê? Porque o worker precisa decidir entre três coisas: **deletar a mensagem**, **deixar para a DLQ**, ou **deixar o SQS reentregar**. Se eu retornasse `errors.New('invalid')`, o worker teria que fazer `strings.Contains` — frágil. Com tipo, é `errors.As`. Idiomático em Go."

### Decisão 3 — Quem retenta o quê (30s)
**Mostre:** `internal/infra/worker/pool.go`, função `handle`.

> "Erro de validação? Não deleto a mensagem. SQS reentrega 3 vezes via `maxReceiveCount`, depois manda pra DLQ. **Não reimplemento backoff exponencial no código** — o broker já faz isso. Reimplementar duplica complexidade e introduz bugs. Backoff no código fica reservado para falhas transitórias de SDK."

---

## 2. Como rodar (1:00)

**Mostre:** o terminal com o `Makefile` aberto ao lado.

> "Pré-requisitos: Docker, Make, Go 1.22 se quiser rodar testes unitários. **Não precisa de AWS CLI local** — o Makefile detecta e cai pra um container `amazon/aws-cli` se necessário."

```bash
make help            # lista todos os alvos
make up              # sobe LocalStack + provisiona + builda e sobe Processor
make test-processor  # testes unitários
make send-test       # publica evento válido
make send-bad        # publica evento inválido (vai pra DLQ)
make logs            # tail do Processor
make down            # tear down completo
```

> "Tudo num único `make up`. LocalStack sobe com healthcheck, um container `aws-init` one-shot cria filas e tabelas, e só depois disso o Processor sobe."

---

## 3. Demo ao vivo (4:30)

### 3.1 — Tear down e subida limpa (45s)

**No terminal:**
```bash
make down
make up
```

**Narração enquanto sobe:**
> "Parto do zero pra mostrar que é determinístico. Repare na ordem: LocalStack fica `Healthy`, depois o `aws-init` cria as filas e tabelas, ele sai com exit code zero, e só então o Processor sobe. Isso é `condition: service_completed_successfully` no compose."

**Mostre:** `docker compose ps` — três containers, dois rodando, um exited com 0.

### 3.2 — Mensagem válida (45s)

```bash
make send-test
```

> "O Makefile gera um UUID novo, pega o timestamp atual em UTC, e publica na `raw-events`. Espera 3 segundos e lê o contador da `processed-events`."

**Espere o output:** `ApproximateNumberOfMessages: "1"`

> "Funcionou. A mensagem foi consumida pelo Processor e publicada já enriquecida na próxima fila."

```bash
docker logs devflow-processor --tail 5
```

> "Logs estruturados em JSON. O campo `event_id` permite correlação ponta a ponta — em produção isso vira chave de busca no Datadog/CloudWatch."

### 3.3 — Mensagem inválida e DLQ (1:30)

```bash
make send-bad
```

> "Esse JSON é deliberadamente quebrado: UUID inválido, `developer_id` vazio, métrica `deploys` que não existe, value negativo, e timestamp em 2099."

```bash
docker logs devflow-processor --tail 5
```

> "Repare: o Processor recusa, mas **não deleta** a mensagem. Loga `validation failed; leaving for DLQ redrive`. Isso é a decisão de delegar retry pro broker."

**Espere ~2 minutos** (visibility timeout 30s × 3 tentativas):

```bash
# verifica DLQ
docker run --rm --network devflow-pipeline_devflow \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  amazon/aws-cli:2.17.0 --endpoint-url=http://localstack:4566 sqs get-queue-attributes \
  --queue-url http://localstack:4566/000000000000/raw-events-dlq \
  --attribute-names ApproximateNumberOfMessages
```

> "1 mensagem na DLQ. Em prod, daqui sai um alarme: 'mensagens estão indo pra DLQ, vai investigar'. O fluxo está correto — mensagem ruim contida, pipeline saudável."

### 3.4 — Tour rápido pelo código (1:00)

**Abra:** `services/processor/internal/domain/event.go`.

> "Domínio: 70 linhas, zero dependência externa além de `uuid`. `Validate(now)` enumera todas as regras do case. Repare no `AllowedFutureSkew`: 5 minutos de tolerância. Sem isso, qualquer diferença de relógio entre produtor e consumidor — coisa real em sistemas distribuídos — derruba mensagens válidas."

**Abra:** `internal/infra/worker/pool.go`, função `Run`.

> "Worker pool: dispatcher faz long-polling, alimenta um channel **sem buffer**, N workers consomem. Channel sem buffer = backpressure natural. Workers ocupados? Dispatcher bloqueia, não busca mais mensagens. Sem isso, em pico de tráfego eu inflaria memória."

**Abra:** `cmd/processor/main.go`, função `run`.

> "Graceful shutdown: `signal.NotifyContext` captura `SIGTERM`. Cancela o context do dispatcher, ele para. `close(jobs)` desencadeia os workers. `wg.Wait()` no fim garante drain antes de sair. Sutileza importante: workers usam `context.Background()` ao chamar o handler, **não o context cancelado** — porque se eu cancelasse no meio do `Publish`, a mensagem voltaria pra fila e seria reprocessada, gerando duplicação na `processed-events`."

### 3.5 — Testes (30s)

```bash
make test-processor
```

> "14 casos no domínio cobrindo todas as regras: UUID, branco, enum, valores negativos, boundary `review_time_minutes` em 1440, futuro, dentro e fora da janela de skew. 3 casos no use case com mock do Publisher: sucesso enriquece, validação curto-circuita, falha de publish é preservada via `errors.Is`. **Não tem teste de integração com LocalStack** — decisão consciente: pelo prazo de 5 dias, o end-to-end via `make send-test` cobre o wiring."

---

## 4. O que faria diferente com mais tempo (1:30)

> "Honestidade técnica:"

**1. Aggregator e API REST.** "A Fase 3 ficou de fora do que dá pra demonstrar aqui hoje [ajuste essa frase pelo seu estado real no momento da gravação]. Idempotência via `ConditionExpression: attribute_not_exists(event_id)` no DynamoDB, agregação incremental via `UpdateItem` com `ADD`. Já estava planejado, ficou como follow-up."

**2. Observabilidade.** "Logging eu cobri. Métricas e tracing não. OpenTelemetry com propagação via `MessageAttributes` do SQS dá um trace ponta a ponta — Processor → fila → Aggregator. Isso transforma debugging em produção. Ficou de fora porque o ROI dentro do prazo de 5 dias era marginal pra um avaliador."

**3. Métrica de DLQ.** "A coisa mais perigosa em pipelines event-driven é mensagens morrerem silenciosamente na DLQ. Eu colocaria uma métrica Prometheus do `ApproximateNumberOfMessages` da DLQ com alerta. Em produção, sem isso, você só descobre o bug quando alguém reclama do dashboard."

**4. Testes de integração.** "Hoje só tenho unit tests. Faria testes de integração contra o LocalStack usando `testcontainers-go` — testa o adapter SQS de verdade, sem mock. Não fiz porque pelo prazo eles são mais frágeis e mais lentos do que o valor que entregam."

**5. Idempotência no Processor.** "O Processor não tem idempotência. Se a mesma mensagem for entregue duas vezes pela `raw-events` (acontece com SQS standard), ela vai aparecer duplicada na `processed-events`. A idempotência está só no Aggregator, antes do DynamoDB. Pra ser robusto de ponta a ponta, faria o Processor publicar com `MessageDeduplicationId` numa FIFO queue, ou aceitaria a duplicação e confiaria no Aggregator. Optei pela segunda — documentado, consciente."

---

## 5. Fechamento (0:30)

> "Resumindo: dois serviços com responsabilidades isoladas, retry delegado ao broker, erros tipados pra decisões de retry/DLQ explícitas, graceful shutdown que preserva mensagens in-flight, e um docker-compose que sobe tudo determinístico. O código tá no GitHub, três PRs documentando a evolução: infra, processor, e os fixes de DX e clock skew. Cada PR tem doc próprio em `docs/`. Obrigado pela atenção."

---

## Cheatsheet de comandos pra deixar aberto numa segunda janela

```bash
# Subida limpa
make down && make up

# Testes
make test-processor

# Mensagem válida
make send-test

# Mensagem inválida (vai pra DLQ depois de ~90s)
make send-bad

# Logs
docker logs devflow-processor --tail 20
docker logs devflow-processor -f       # streaming

# Inspecionar filas
docker run --rm --network devflow-pipeline_devflow \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  amazon/aws-cli:2.17.0 --endpoint-url=http://localstack:4566 \
  sqs list-queues

# Atributos da DLQ
docker run --rm --network devflow-pipeline_devflow \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  amazon/aws-cli:2.17.0 --endpoint-url=http://localstack:4566 \
  sqs get-queue-attributes \
  --queue-url http://localstack:4566/000000000000/raw-events-dlq \
  --attribute-names ApproximateNumberOfMessages
```

---

## Dicas finais de gravação

1. **Ensaie o bloco 3 (demo) duas vezes** antes de gravar — é onde mais coisa pode dar errado.
2. **Pré-aqueça o Docker:** rode `make up` uma vez antes da gravação pra cachear images. Pra gravar, faça `make down && make up` — vai ser rápido.
3. **Fonte grande no terminal** (16pt+). Avaliador vai assistir em laptop, não em monitor 4K.
4. **Pause entre comandos.** Não dispare 3 comandos seguidos — cada um precisa de 2-3s de fala explicando.
5. **Não tente esconder erros se aparecer um.** Se algo quebrar, explica em tempo real o que está acontecendo. **É exatamente o que avaliador quer ver** — como você raciocina sob pressão.
6. **Não edite o vídeo.** Gravação contínua é mais autêntica e o case explicitamente diz "valorizamos clareza, não produção".
