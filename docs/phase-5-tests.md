# Fase 5 — Testes

## Objetivo

Cobrir as regras de negócio dos dois serviços com testes rápidos, determinísticos e sem dependência de infra. LocalStack fica para o `verify-e2e` (Fase 6) — aqui o foco é unidade + handler com `httptest`.

## Estratégia

Três camadas testadas, uma de fora:

| Camada | Como é testada | Por quê |
|---|---|---|
| `domain` | Tabela de casos puros (`event_test.go`) | Validação é regra de negócio; testes diretos, sem mock. |
| `usecase` | Mocks dos ports (`SQSAPI`, `Repository`, `Publisher`) | Verifica orquestração (validar → persistir → publicar / idempotência / erros transitórios) sem rede. |
| `infra/api` | `httptest.NewServer` + `usecase` real com repositório fake | Garante contrato HTTP (status codes, payloads, headers) sem subir DynamoDB. |
| `infra/queue`, `infra/repo` (DynamoDB) | **Não testado em unit** — coberto pelo `verify-e2e` contra LocalStack | Não vale a pena mockar o SDK AWS; o teste de integração end-to-end é mais barato e mais real. |

## Inventário

```
services/processor/
├── internal/domain/event_test.go              # validação (tabela: tipo, campos obrigatórios, future timestamp)
├── internal/usecase/process_event_test.go     # sucesso, erro de validação (sem publish), erro de publish, idempotência por event_id
└── internal/infra/worker/pool_test.go         # dispatcher distribui mensagens, drain limpo no Stop()

services/aggregator/
├── internal/domain/event_test.go              # parsing de ProcessedEvent + summary update
├── internal/usecase/aggregate_event_test.go   # persist + summary increment, duplicado é no-op, review_time agrega só com duration_minutes
└── internal/infra/api/
    ├── handlers_test.go                       # GET /metrics/{id}, /summary, /health (200, 404, 503)
    └── docs_test.go                           # /openapi.yaml com content-type correto + /docs serve ReDoc
```

Total: ~717 linhas de teste, 20 funções `Test*`.

## Como rodar

```bash
make test               # processor + aggregator
make test-processor
make test-aggregator
```

Sem flags exóticas — `go test ./...` em cada módulo. Sem rede, sem Docker, sem fixtures externos. Cada arquivo de teste roda isolado em < 1 s.

## Decisões

- **Mocks à mão, não `gomock`/`testify-mock`.** As interfaces são pequenas (3-4 métodos) e os testes ficam mais legíveis com structs implementando as ports diretamente. Sem geração de código, sem dependência extra.
- **`httptest` em vez de subir o servidor.** Router é uma função pura `NewRouter(handler, mws...) http.Handler` — `httptest.NewServer` exercita o stack inteiro de middleware sem socket de verdade.
- **Sem cobertura mínima como gate.** O case pede testes "onde fizer sentido". A linha foi: regra de negócio + contrato HTTP cobertos; wrapping de SDK AWS fica para o `verify-e2e`.
- **Sem flaky controls.** Nenhum teste usa `time.Sleep`, `goroutine` sem `sync.WaitGroup`, ou ordem de map. CI determinístico.
