# syntax=docker/dockerfile:1.7
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/aggregator ./cmd/aggregator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/aggregator /aggregator
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/aggregator"]
