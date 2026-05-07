package domain

import "time"

// SupportedSchemaVersion is the only schema version this aggregator
// understands. Anything else is rejected as UnsupportedSchemaError.
const SupportedSchemaVersion = 1

// retentionWindow is the TTL applied to events table rows. See D2.
const retentionWindow = 30 * 24 * time.Hour

// ProcessedEvent mirrors the contract published by the processor on
// the processed-events queue. The fields are replicated (rather than
// embedded from a shared package) so the aggregator owns its own
// contract — the JSON shape is the inter-service boundary.
type ProcessedEvent struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	DeveloperID   string    `json:"developer_id"`
	MetricType    string    `json:"metric_type"`
	Value         float64   `json:"value"`
	Repository    string    `json:"repository"`
	Timestamp     time.Time `json:"timestamp"`
	ProcessedAt   time.Time `json:"processed_at"`
	ProcessorID   string    `json:"processor_id"`
}

// EventRecord is the row persisted to the events DynamoDB table.
type EventRecord struct {
	EventID     string  `dynamodbav:"event_id"`
	DeveloperID string  `dynamodbav:"developer_id"`
	MetricType  string  `dynamodbav:"metric_type"`
	Value       float64 `dynamodbav:"value"`
	Repository  string  `dynamodbav:"repository"`
	Timestamp   string  `dynamodbav:"timestamp"`
	ProcessedAt string  `dynamodbav:"processed_at"`
	ProcessorID string  `dynamodbav:"processor_id"`
	ExpiresAt   int64   `dynamodbav:"expires_at"`
}

// ToRecord builds the persistence record for an event. ExpiresAt is
// derived from ProcessedAt + 30d (D2).
func (e ProcessedEvent) ToRecord() EventRecord {
	return EventRecord{
		EventID:     e.EventID,
		DeveloperID: e.DeveloperID,
		MetricType:  e.MetricType,
		Value:       e.Value,
		Repository:  e.Repository,
		Timestamp:   e.Timestamp.UTC().Format(time.RFC3339Nano),
		ProcessedAt: e.ProcessedAt.UTC().Format(time.RFC3339Nano),
		ProcessorID: e.ProcessorID,
		ExpiresAt:   e.ProcessedAt.Add(retentionWindow).Unix(),
	}
}

// MetricType constants — kept as plain strings to mirror the JSON wire
// format without coupling to the processor's typed alias.
const (
	MetricCommits           = "commits"
	MetricPullRequests      = "pull_requests"
	MetricReviewTimeMinutes = "review_time_minutes"
)

// SummaryIncrements describes the ADD-style deltas to apply to a
// developer_summary row for a single event.
type SummaryIncrements struct {
	Commits           int64
	PullRequests      int64
	ReviewTimeMinutes float64
	ReviewTimeCount   int64
	Now               time.Time
	EventTimestamp    time.Time
}

// IncrementsFor derives the per-metric deltas from a ProcessedEvent.
func IncrementsFor(e ProcessedEvent, now time.Time) SummaryIncrements {
	inc := SummaryIncrements{Now: now, EventTimestamp: e.Timestamp}
	switch e.MetricType {
	case MetricCommits:
		inc.Commits = int64(e.Value)
	case MetricPullRequests:
		inc.PullRequests = int64(e.Value)
	case MetricReviewTimeMinutes:
		inc.ReviewTimeMinutes = e.Value
		inc.ReviewTimeCount = 1
	}
	return inc
}

// DeveloperSummary is the materialised view per developer (D4).
type DeveloperSummary struct {
	DeveloperID            string  `dynamodbav:"developer_id" json:"developer_id"`
	TotalCommits           int64   `dynamodbav:"total_commits" json:"total_commits"`
	TotalPullRequests      int64   `dynamodbav:"total_pull_requests" json:"total_pull_requests"`
	TotalReviewTimeMinutes float64 `dynamodbav:"total_review_time_minutes" json:"total_review_time_minutes"`
	ReviewTimeCount        int64   `dynamodbav:"review_time_count" json:"review_time_count"`
	EventsProcessed        int64   `dynamodbav:"events_processed" json:"events_processed"`
	LastActivity           string  `dynamodbav:"last_activity" json:"last_activity"`
	UpdatedAt              string  `dynamodbav:"updated_at" json:"updated_at"`
}

// AvgReviewTimeMinutes computes the average defensively; with no
// review-time samples it returns 0 instead of dividing by zero.
func (s DeveloperSummary) AvgReviewTimeMinutes() float64 {
	if s.ReviewTimeCount == 0 {
		return 0
	}
	return s.TotalReviewTimeMinutes / float64(s.ReviewTimeCount)
}
