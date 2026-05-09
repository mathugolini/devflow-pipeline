package api

import (
	_ "embed"
	"net/http"
)

// openapiSpec is the YAML contract for the API. Embedded so the binary
// is self-contained — the same image works in any environment without
// shipping a sidecar file.
//
//go:embed openapi.yaml
var openapiSpec []byte

// docsHTML renders the spec via ReDoc (single <script> tag from a CDN).
// ReDoc is preferred over Swagger UI here because it boots from one
// resource, has no build step, and renders the YAML as-is.
const docsHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>devflow-pipeline — Aggregator API</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>body{margin:0;padding:0}</style>
</head>
<body>
  <redoc spec-url="/openapi.yaml"></redoc>
  <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
</body>
</html>`

// OpenAPI returns the embedded YAML contract.
func (h *Handler) OpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(openapiSpec)
}

// Docs serves a ReDoc-rendered HTML page that fetches /openapi.yaml.
// Network access is required to load the JS bundle from the CDN.
func (h *Handler) Docs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write([]byte(docsHTML))
}
