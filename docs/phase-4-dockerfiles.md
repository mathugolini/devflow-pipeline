# Fase 4 — Dockerfiles multi-stage

> Entregue em conjunto com as Fases 2 e 3.

## Objetivo

Produzir imagens finais pequenas, seguras e reproduzíveis para `processor` e `aggregator`, alinhadas aos requisitos do case: multi-stage, base distroless, usuário não-root, binário estático.

## Receita comum

Ambos os serviços usam o mesmo `Dockerfile` (variando apenas o nome do comando):

```dockerfile
# syntax=docker/dockerfile:1.7
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/<svc> ./cmd/<svc>

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/<svc> /<svc>
USER nonroot:nonroot
ENTRYPOINT ["/<svc>"]
```

Fontes: [services/processor/Dockerfile](services/processor/Dockerfile), [services/aggregator/Dockerfile](services/aggregator/Dockerfile).

## Decisões e por quê

| Escolha | Motivo |
|---|---|
| `golang:1.22-alpine` no builder | Imagem oficial, pin de minor; alpine reduz tempo de pull do builder (descartado no estágio final). |
| `CGO_ENABLED=0` | Binário estático puro — roda sem libc na imagem final distroless `static`. |
| `-trimpath` | Remove caminhos absolutos do host do binário — builds reproduzíveis e sem leak de paths internos. |
| `-ldflags="-s -w"` | Strip de tabela de símbolos e DWARF; reduz o binário em ~30 %. |
| `go mod download` antes do `COPY . .` | Camada de cache de deps separada do código — rebuild de código não rebaixa deps. |
| `gcr.io/distroless/static-debian12:nonroot` | Sem shell, sem package manager, sem libc. UID/GID `65532:65532` aplicados via tag `:nonroot`. Superfície de ataque mínima. |
| `USER nonroot:nonroot` explícito | Defesa em profundidade: mesmo que a tag mude, o `USER` continua válido. |
| `ENTRYPOINT` direto no binário | Sem `sh -c`, sem PID 1 indireto — sinais (`SIGTERM` do `docker stop`) chegam direto no processo Go, que faz graceful shutdown. |

## Resultado mensurável

```
$ docker images devflow-*
REPOSITORY              TAG       SIZE
devflow-aggregator      latest    ~17MB
devflow-processor       latest    ~16MB
```

Comparativo: a mesma binary em `golang:1.22` (sem distroless) fica em ~870 MB.

## O que não está aqui (e por quê)

- **Healthcheck dentro do Dockerfile.** Healthcheck do compose chama o endpoint `/health` da API (aggregator) e o `localstack:4566` para o processor indiretamente via `depends_on: condition: service_completed_successfully` do `aws-init`. Não duplicamos no Dockerfile.
- **`HEALTHCHECK` via `CMD curl`.** Imagem `distroless/static` não tem `curl`. A alternativa seria embarcar um binário `grpc_health_probe` — overkill para o escopo.
- **Multi-arch.** Build local x86_64; produção precisaria de `docker buildx --platform linux/amd64,linux/arm64`.
