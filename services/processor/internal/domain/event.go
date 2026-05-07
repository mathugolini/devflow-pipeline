package domain

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

type MetricType string

const (
	MetricCommits           MetricType = "commits"
	MetricPullRequests      MetricType = "pull_requests"
	MetricReviewTimeMinutes MetricType = "review_time_minutes"
)

func (m MetricType) Valid() bool {
	switch m {
	case MetricCommits, MetricPullRequests, MetricReviewTimeMinutes:
		return true
	}
	return false
}

const (
	reviewTimeMaxMinutes = 1440 // 24h

	// AllowedFutureSkew tolerates clock differences between event
	// producers and the processor. Without it, a producer running a
	// few seconds ahead would have all its events rejected.
	AllowedFutureSkew = 5 * time.Minute

	maxDeveloperIDLen = 128
	maxRepositoryLen  = 256
)

// RawEvent is the contract on the raw-events queue.
type RawEvent struct {
	EventID     string     `json:"event_id"`
	DeveloperID string     `json:"developer_id"`
	MetricType  MetricType `json:"metric_type"`
	Value       float64    `json:"value"`
	Repository  string     `json:"repository"`
	Timestamp   time.Time  `json:"timestamp"`
}

// Validate enforces every business rule listed in the case.
// Returns a *ValidationError on failure.
func (e RawEvent) Validate(now time.Time) error {
	if e.EventID == "" {
		return newValidationError("event_id", "is required")
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return newValidationError("event_id", "must be a valid UUID")
	}
	if strings.TrimSpace(e.DeveloperID) == "" {
		return newValidationError("developer_id", "is required")
	}
	if len(e.DeveloperID) > maxDeveloperIDLen {
		return newValidationError("developer_id", "exceeds 128 chars")
	}
	if len(e.Repository) > maxRepositoryLen {
		return newValidationError("repository", "exceeds 256 chars")
	}
	if !e.MetricType.Valid() {
		return newValidationError("metric_type", "must be one of commits|pull_requests|review_time_minutes")
	}
	if e.Value < 0 {
		return newValidationError("value", "must be >= 0")
	}
	if e.MetricType == MetricReviewTimeMinutes && e.Value > reviewTimeMaxMinutes {
		return newValidationError("value", "review_time_minutes cannot exceed 1440")
	}
	if e.Timestamp.IsZero() {
		return newValidationError("timestamp", "is required")
	}
	if e.Timestamp.After(now.Add(AllowedFutureSkew)) {
		return newValidationError("timestamp", "cannot be in the future")
	}
	return nil
}

// ProcessedEvent is the contract on the processed-events queue.
type ProcessedEvent struct {
	RawEvent
	ProcessedAt time.Time `json:"processed_at"`
	ProcessorID string    `json:"processor_id"`
}
