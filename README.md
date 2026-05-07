# devflow-pipeline

Pipeline de métricas de produtividade de desenvolvedores em Go.
Dois serviços comunicando via SQS, persistindo em DynamoDB, expostos via API REST. Tudo roda em LocalStack.

```
[SQS: raw-events] → [Processor] → [SQS: processed-events] → [Aggregator] → [DynamoDB] → [API REST]
```

## Status atual

- [x] Fase 1 — Infra (LocalStack + provisionamento automático de SQS/DynamoDB) — [doc](docs/phase-1-infrastructure.md)
- [x] Fase 2 — Processor (validação, worker pool, graceful shutdown, Dockerfile distroless) — [doc](docs/phase-2-processor.md)
- [ ] Fase 3 — Aggregator
- [ ] Fase 4 — Dockerfiles multi-stage + integração no compose
- [ ] Fase 5 — Testes
- [ ] Fase 6 — Seed e validação end-to-end

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
