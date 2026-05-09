package api

import "net/http"

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
	return handler
}
