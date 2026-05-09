package api

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// NewRouter wires the HTTP routes using Go 1.22 method-aware mux. The
// variadic middlewares are applied in order: mws[0] is the outermost
// wrapper, so request logging / auth can compose naturally.
func NewRouter(h *Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("GET /metrics/{developer_id}", h.ListEvents)
	mux.HandleFunc("GET /metrics/{developer_id}/summary", h.Summary)
	mux.HandleFunc("GET /openapi.yaml", h.OpenAPI)
	mux.HandleFunc("GET /docs", h.Docs)

	var handler http.Handler = mux
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	// Outermost wrapper: extracts trace context from incoming requests
	// and emits a server span per request. Span name is the route
	// pattern (set via WithRouteTag at the mux level if needed); the
	// default — "HTTP <method>" — keeps cardinality bounded.
	return otelhttp.NewHandler(handler, "aggregator")
}
