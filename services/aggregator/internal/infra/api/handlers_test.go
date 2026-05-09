package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

type fakeReader struct {
	events  []domain.EventRecord
	listErr error

	summary    domain.DeveloperSummary
	found      bool
	summaryErr error
}

func (f *fakeReader) ListByDeveloper(_ context.Context, _ string) ([]domain.EventRecord, error) {
	return f.events, f.listErr
}
func (f *fakeReader) GetSummary(_ context.Context, _ string) (domain.DeveloperSummary, bool, error) {
	return f.summary, f.found, f.summaryErr
}

type fakePinger struct{ sqsErr, ddbErr error }

func (f *fakePinger) PingSQS(_ context.Context) error { return f.sqsErr }
func (f *fakePinger) PingDDB(_ context.Context) error { return f.ddbErr }

func newServer(r Reader, p Pinger) *httptest.Server {
	return httptest.NewServer(NewRouter(NewHandler(r, p)))
}

func TestHealth(t *testing.T) {
	cases := []struct {
		name     string
		pinger   *fakePinger
		wantCode int
		wantSub  string
	}{
		{"all ok", &fakePinger{}, 200, `"status":"ok"`},
		{"sqs down", &fakePinger{sqsErr: errors.New("sqs boom")}, 503, "sqs boom"},
		{"ddb down", &fakePinger{ddbErr: errors.New("ddb boom")}, 503, "ddb boom"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := newServer(&fakeReader{}, c.pinger)
			defer srv.Close()
			resp, err := http.Get(srv.URL + "/health")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != c.wantCode {
				t.Errorf("status = %d, want %d", resp.StatusCode, c.wantCode)
			}
			buf := readAll(t, resp)
			if !strings.Contains(buf, c.wantSub) {
				t.Errorf("body missing %q: %s", c.wantSub, buf)
			}
		})
	}
}

func TestListEvents(t *testing.T) {
	rdr := &fakeReader{events: []domain.EventRecord{
		{EventID: "e1", DeveloperID: "dev-1", MetricType: "commits", Value: 2},
	}}
	srv := newServer(rdr, &fakePinger{})
	defer srv.Close()

	t.Run("ok", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/metrics/dev-1")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		var got []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0]["event_id"] != "e1" {
			t.Errorf("unexpected body: %v", got)
		}
	})

	t.Run("empty list returns []", func(t *testing.T) {
		empty := newServer(&fakeReader{events: nil}, &fakePinger{})
		defer empty.Close()
		resp, err := http.Get(empty.URL + "/metrics/dev-2")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("want 200 got %d", resp.StatusCode)
		}
		body := readAll(t, resp)
		if strings.TrimSpace(body) != "[]" {
			t.Errorf("want [], got %q", body)
		}
	})
}

func TestSummary(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		srv := newServer(&fakeReader{found: false}, &fakePinger{})
		defer srv.Close()
		resp, err := http.Get(srv.URL + "/metrics/dev-1/summary")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Errorf("want 404 got %d", resp.StatusCode)
		}
		body := readAll(t, resp)
		if !strings.Contains(body, `"error":"not found"`) {
			t.Errorf("body missing error: %s", body)
		}
	})

	t.Run("ok with avg zero guard", func(t *testing.T) {
		srv := newServer(&fakeReader{found: true, summary: domain.DeveloperSummary{
			DeveloperID:            "dev-1",
			TotalCommits:           5,
			TotalReviewTimeMinutes: 0,
			ReviewTimeCount:        0,
		}}, &fakePinger{})
		defer srv.Close()
		resp, err := http.Get(srv.URL + "/metrics/dev-1/summary")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("want 200 got %d", resp.StatusCode)
		}
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body["avg_review_time_minutes"].(float64) != 0 {
			t.Errorf("avg should be 0 with zero count, got %v", body["avg_review_time_minutes"])
		}
		if body["total_commits"].(float64) != 5 {
			t.Errorf("total_commits = %v, want 5", body["total_commits"])
		}
	})

	t.Run("ok with avg computed", func(t *testing.T) {
		srv := newServer(&fakeReader{found: true, summary: domain.DeveloperSummary{
			DeveloperID:            "dev-1",
			TotalReviewTimeMinutes: 30,
			ReviewTimeCount:        3,
		}}, &fakePinger{})
		defer srv.Close()
		resp, err := http.Get(srv.URL + "/metrics/dev-1/summary")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body["avg_review_time_minutes"].(float64) != 10 {
			t.Errorf("avg = %v, want 10", body["avg_review_time_minutes"])
		}
	})
}

func readAll(t *testing.T, r *http.Response) string {
	t.Helper()
	b := make([]byte, 0, 1024)
	buf := make([]byte, 1024)
	for {
		n, err := r.Body.Read(buf)
		if n > 0 {
			b = append(b, buf[:n]...)
		}
		if err != nil {
			break
		}
	}
	return string(b)
}
