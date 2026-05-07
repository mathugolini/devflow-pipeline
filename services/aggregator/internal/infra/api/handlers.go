// Package api exposes the aggregator's read-only HTTP surface.
// Endpoints (D6, no auth — demo only):
//
//	GET /health
//	GET /metrics/{developer_id}
//	GET /metrics/{developer_id}/summary
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

// Reader is the inbound port for the API.
type Reader interface {
	ListEventsByDeveloper(ctx context.Context, developerID string) ([]domain.EventRecord, error)
	GetSummary(ctx context.Context, developerID string) (domain.DeveloperSummary, bool, error)
}

// Pinger covers the dependencies the /health endpoint probes.
type Pinger interface {
	PingSQS(ctx context.Context) error
	PingDDB(ctx context.Context) error
}

type Handler struct {
	reader Reader
	pinger Pinger
}

func NewHandler(r Reader, p Pinger) *Handler {
	return &Handler{reader: r, pinger: p}
}

type healthResp struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{"sqs": "ok", "dynamodb": "ok"}
	ok := true
	if err := h.pinger.PingSQS(r.Context()); err != nil {
		checks["sqs"] = err.Error()
		ok = false
	}
	if err := h.pinger.PingDDB(r.Context()); err != nil {
		checks["dynamodb"] = err.Error()
		ok = false
	}
	resp := healthResp{Status: "ok", Checks: checks}
	if !ok {
		resp.Status = "degraded"
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type apiEvent struct {
	EventID     string  `json:"event_id"`
	DeveloperID string  `json:"developer_id"`
	MetricType  string  `json:"metric_type"`
	Value       float64 `json:"value"`
	Repository  string  `json:"repository"`
	Timestamp   string  `json:"timestamp"`
	ProcessedAt string  `json:"processed_at"`
	ProcessorID string  `json:"processor_id"`
}

func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	dev := r.PathValue("developer_id")
	if dev == "" {
		writeError(w, http.StatusBadRequest, "developer_id required")
		return
	}
	recs, err := h.reader.ListEventsByDeveloper(r.Context(), dev)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]apiEvent, 0, len(recs))
	for _, rec := range recs {
		out = append(out, apiEvent{
			EventID:     rec.EventID,
			DeveloperID: rec.DeveloperID,
			MetricType:  rec.MetricType,
			Value:       rec.Value,
			Repository:  rec.Repository,
			Timestamp:   rec.Timestamp,
			ProcessedAt: rec.ProcessedAt,
			ProcessorID: rec.ProcessorID,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type summaryResp struct {
	DeveloperID            string  `json:"developer_id"`
	TotalCommits           int64   `json:"total_commits"`
	TotalPullRequests      int64   `json:"total_pull_requests"`
	AvgReviewTimeMinutes   float64 `json:"avg_review_time_minutes"`
	TotalReviewTimeMinutes float64 `json:"total_review_time_minutes"`
	ReviewTimeCount        int64   `json:"review_time_count"`
	EventsProcessed        int64   `json:"events_processed"`
	LastActivity           string  `json:"last_activity"`
	UpdatedAt              string  `json:"updated_at"`
}

func (h *Handler) Summary(w http.ResponseWriter, r *http.Request) {
	dev := r.PathValue("developer_id")
	if dev == "" {
		writeError(w, http.StatusBadRequest, "developer_id required")
		return
	}
	s, found, err := h.reader.GetSummary(r.Context(), dev)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, summaryResp{
		DeveloperID:            s.DeveloperID,
		TotalCommits:           s.TotalCommits,
		TotalPullRequests:      s.TotalPullRequests,
		AvgReviewTimeMinutes:   s.AvgReviewTimeMinutes(),
		TotalReviewTimeMinutes: s.TotalReviewTimeMinutes,
		ReviewTimeCount:        s.ReviewTimeCount,
		EventsProcessed:        s.EventsProcessed,
		LastActivity:           s.LastActivity,
		UpdatedAt:              s.UpdatedAt,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ErrNotFound is exported in case other layers want to map storage
// "not found" to the HTTP layer without typed errors.
var ErrNotFound = errors.New("not found")
