package domain

import (
	"testing"
	"time"
)

func TestProcessedEvent_ToRecord_DerivesExpiresAt(t *testing.T) {
	processedAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	e := ProcessedEvent{
		SchemaVersion: 1,
		EventID:       "ev1",
		DeveloperID:   "dev-1",
		MetricType:    MetricCommits,
		Value:         3,
		Repository:    "org/repo",
		Timestamp:     processedAt.Add(-time.Minute),
		ProcessedAt:   processedAt,
		ProcessorID:   "p1",
	}
	rec := e.ToRecord()

	want := processedAt.Add(30 * 24 * time.Hour).Unix()
	if rec.ExpiresAt != want {
		t.Errorf("ExpiresAt = %d, want %d", rec.ExpiresAt, want)
	}
	if rec.EventID != "ev1" || rec.DeveloperID != "dev-1" {
		t.Errorf("identity fields not copied: %+v", rec)
	}
	if rec.Value != 3 {
		t.Errorf("Value = %v, want 3", rec.Value)
	}
}

func TestIncrementsFor(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		evt  ProcessedEvent
		want SummaryIncrements
	}{
		{
			name: "commits",
			evt:  ProcessedEvent{MetricType: MetricCommits, Value: 5},
			want: SummaryIncrements{Commits: 5},
		},
		{
			name: "pull_requests",
			evt:  ProcessedEvent{MetricType: MetricPullRequests, Value: 2},
			want: SummaryIncrements{PullRequests: 2},
		},
		{
			name: "review_time_minutes",
			evt:  ProcessedEvent{MetricType: MetricReviewTimeMinutes, Value: 17.5},
			want: SummaryIncrements{ReviewTimeMinutes: 17.5, ReviewTimeCount: 1},
		},
		{
			name: "unknown metric ignored",
			evt:  ProcessedEvent{MetricType: "garbage", Value: 99},
			want: SummaryIncrements{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := IncrementsFor(c.evt, now)
			if got.Commits != c.want.Commits ||
				got.PullRequests != c.want.PullRequests ||
				got.ReviewTimeMinutes != c.want.ReviewTimeMinutes ||
				got.ReviewTimeCount != c.want.ReviewTimeCount {
				t.Errorf("got %+v want (subset) %+v", got, c.want)
			}
			if !got.Now.Equal(now) {
				t.Errorf("Now not propagated")
			}
		})
	}
}

func TestDeveloperSummary_AvgReviewTimeMinutes_ZeroGuard(t *testing.T) {
	s := DeveloperSummary{TotalReviewTimeMinutes: 100, ReviewTimeCount: 0}
	if got := s.AvgReviewTimeMinutes(); got != 0 {
		t.Errorf("avg = %v, want 0 (zero guard)", got)
	}

	s2 := DeveloperSummary{TotalReviewTimeMinutes: 100, ReviewTimeCount: 4}
	if got := s2.AvgReviewTimeMinutes(); got != 25 {
		t.Errorf("avg = %v, want 25", got)
	}
}
