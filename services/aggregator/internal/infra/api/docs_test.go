package api

import (
	"net/http"
	"strings"
	"testing"
)

func TestOpenAPI_ServesEmbeddedYAML(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakePinger{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/yaml") {
		t.Errorf("content-type = %q, want application/yaml*", ct)
	}
	body := readAll(t, resp)
	// Spot-check that the spec really embedded — title + the 3 paths.
	for _, want := range []string{
		"openapi: 3.1.0",
		"Aggregator API",
		"/health",
		"/metrics/{developer_id}",
		"/metrics/{developer_id}/summary",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("openapi.yaml missing %q", want)
		}
	}
}

func TestDocs_ServesRedocHTML(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakePinger{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/docs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content-type = %q, want text/html*", ct)
	}
	body := readAll(t, resp)
	if !strings.Contains(body, `spec-url="/openapi.yaml"`) {
		t.Errorf("/docs HTML must point ReDoc at /openapi.yaml; got: %s", body[:min(200, len(body))])
	}
}
