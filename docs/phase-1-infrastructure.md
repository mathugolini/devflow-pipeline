# Fase 1 — Infraestrutura (LocalStack + provisionamento)

> PR: [#1](https://github.com/mathugolini/devflow-pipeline/pull/1) · Branch: `claude/elastic-fermi-f7e0ee` (merged em `main`)

## Objetivo

Subir a base de infra (SQS + DynamoDB) de forma **automática, idempotente e validável** antes de qualquer linha de Go. Se o `docker-compose up` não consegue criar as filas e tabelas sozinho, nenhum serviço pode ser testado.

## Componentes entregues

| Arquivo | Papel |
| --- | --- |
| `docker-compose.yml` | Sobe LocalStack com healthcheck + container `aws-init` one-shot |
| `infra/localstack/init-aws.sh` | Provisiona filas (com DLQ) e tabelas DynamoDB |
| `Makefile` | `up`, `down`, `verify-infra`, `logs`, `ps`, `clean` |
| `.env.example` | Contrato de variáveis de ambiente para os serviços |
| `.gitignore` | Cobre binários Go, volumes do LocalStack, IDE, env locais |
| `README.md` | Status das fases + instruções de execução |

## Arquitetura desta fase

```
┌──────────────────────────────────────────────────────────────┐
│ docker compose                                               │
│                                                              │
│  ┌──────────────┐  healthy   ┌───────────────────┐           │
│  │  localstack  │ ─────────▶ │     aws-init      │ exit 0    │
│  │  SQS+DynamoDB│            │  (amazon/aws-cli) │           │
│  │  :4566       │            │                   │           │
│  └──────────────┘            └───────────────────┘           │
│         ▲                              │                     │
│         │  cria filas/tabelas          │                     │
│         └──────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────┘
```

## Decisões de design (e por quê)

### 1. `aws-init` como container one-shot separado, não hook do LocalStack

**O que considerei:**
- (A) Usar `init/ready.d` do LocalStack — script copiado pra dentro do container e executado no startup.
- (B) **Container separado `amazon/aws-cli` que depende de `service_healthy` do LocalStack.** ✅

**Por que B:**
- Logs isolados — `docker compose logs aws-init` mostra exatamente o provisionamento, separado do ruído do LocalStack.
- Falha visível: se o script quebra, o container sai com exit code != 0 e `docker compose run --rm aws-init` falha de forma determinística.
- Reutilizável em CI/CD ou para apontar pra AWS real só trocando `AWS_ENDPOINT_URL`.
- Não acopla o provisionamento ao ciclo de vida do LocalStack.

**Trade-off:** requer um pull adicional (`amazon/aws-cli`, ~400MB). Aceitável: roda uma vez.

### 2. Healthcheck real em vez de `sleep`

**O que considerei:**
- (A) `sleep 10` antes de rodar o init.
- (B) **Healthcheck no container LocalStack + `condition: service_healthy` no compose + polling defensivo no script (60×1s).** ✅

**Por que B:**
- `sleep` é frágil: máquina lenta = race condition; máquina rápida = espera inútil.
- Healthcheck oficial (`/_localstack/health`) reflete o estado real do serviço.
- O polling no script é **defesa em profundidade**: se alguém rodar o `init-aws.sh` fora do compose, ele ainda funciona.

**Trade-off:** três camadas de espera. É verboso de manter, mas o custo de uma race condition em demo é altíssimo.

### 3. Idempotência no `init-aws.sh`

Antes de criar qualquer recurso, o script verifica se já existe (`get-queue-url`, `describe-table`).

**Por quê:** rodar `make up` duas vezes não pode quebrar. Em LocalStack com `PERSISTENCE=0` isso quase não importa, mas o hábito previne bugs quando o ambiente persistir.

### 4. DLQs criadas **antes** das filas principais

A `RedrivePolicy` precisa do **ARN da DLQ existente**. Criar `raw-events` antes de `raw-events-dlq` quebra com `InvalidAttributeValue`.

Decisão: ordem fixa no script, com um helper `dlq_arn()` que monta o ARN explicitamente em vez de depender do retorno do create-queue.

### 5. `VisibilityTimeout=30s` definido **na fila**, não no consumer

**Trade-off:** poderia ser configurado no `ReceiveMessage` do código Go (com `VisibilityTimeout` por chamada).

**Por que na fila:**
- Source of truth única — todo consumer respeita.
- 30s é tempo suficiente para o pior caso de processamento (publish na próxima fila).
- Quando o caso usa long polling com `WaitTimeSeconds=10`, sobram 20s reais para processar — folga generosa.

### 6. `PERSISTENCE=0` no LocalStack

**Trade-off:** cada `make up` parte de zero. Em produção isso seria errado; em demo é o comportamento desejado — reprodutível, sem estado pendurado.

**Documentado no README** como decisão consciente.

### 7. Dois `go.mod` (decisão de longo prazo, mas tomada aqui)

Isso afeta a infra porque os Dockerfiles de cada serviço terão `WORKDIR services/<svc>` e cada um copia seu próprio `go.mod`/`go.sum`. Decisão: **um módulo por serviço.**

**Por quê:**
- Força contrato explícito entre Processor e Aggregator via JSON (não via pacote Go compartilhado).
- Imagens Docker menores (cada serviço só baixa suas deps).
- Reflete a separação real do case (dois serviços independentes).

**Trade-off:** structs de evento serão duplicadas. **Isso é intencional** — é o contrato da fila, não código compartilhado.

## O que ficou de fora desta fase

- Containers Go (vão entrar conforme cada serviço fica pronto).
- Seed script (`scripts/seed.sh`) — vai junto com a Fase 6.
- Observabilidade (OpenTelemetry, métricas) — diferencial, fica para o final se sobrar tempo.

## Como validar

```bash
make up            # sobe LocalStack + provisiona
make verify-infra  # lista filas, tabelas e RedrivePolicy
make down          # tear down completo
```

**Resultado validado:** 4 filas, 2 tabelas, redrive policy correta na `raw-events`. Confirmado em execução real.

## O que eu faria diferente com mais tempo

- **Terraform/Pulumi** em vez de bash, mesmo para LocalStack — mesmo IaC funcionando local e em prod.
- **Healthcheck custom** que valida não só a porta 4566 mas também que SQS e DynamoDB estão prontos (LocalStack às vezes responde no health antes de todos os serviços estarem up).
- **Versão do LocalStack pinada via digest** (`localstack/localstack@sha256:...`) para reprodutibilidade total.
