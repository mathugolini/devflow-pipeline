package domain

import (
	"testing"
	"time"
)

func validEvent(now time.Time) RawEvent {
	return RawEvent{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-123",
		MetricType:  MetricCommits,
		Value:       3,
		Repository:  "org/repo",
		Timestamp:   now.Add(-time.Minute),
	}
}

func TestRawEvent_Validate(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		mutate  func(*RawEvent)
		wantErr bool
		field   string
	}{
		{name: "valid event", mutate: func(e *RawEvent) {}, wantErr: false},
		{name: "missing event_id", mutate: func(e *RawEvent) { e.EventID = "" }, wantErr: true, field: "event_id"},
		{name: "invalid uuid", mutate: func(e *RawEvent) { e.EventID = "not-a-uuid" }, wantErr: true, field: "event_id"},
		{name: "missing developer_id", mutate: func(e *RawEvent) { e.DeveloperID = "" }, wantErr: true, field: "developer_id"},
		{name: "blank developer_id", mutate: func(e *RawEvent) { e.DeveloperID = "   " }, wantErr: true, field: "developer_id"},
		{name: "invalid metric_type", mutate: func(e *RawEvent) { e.MetricType = "deploys" }, wantErr: true, field: "metric_type"},
		{name: "negative value", mutate: func(e *RawEvent) { e.Value = -1 }, wantErr: true, field: "value"},
		{name: "review_time over 1440", mutate: func(e *RawEvent) {
			e.MetricType = MetricReviewTimeMinutes
			e.Value = 1441
		}, wantErr: true, field: "value"},
		{name: "review_time at boundary", mutate: func(e *RawEvent) {
			e.MetricType = MetricReviewTimeMinutes
			e.Value = 1440
		}, wantErr: false},
		{name: "missing timestamp", mutate: func(e *RawEvent) { e.Timestamp = time.Time{} }, wantErr: true, field: "timestamp"},
		{name: "future timestamp", mutate: func(e *RawEvent) { e.Timestamp = now.Add(time.Hour) }, wantErr: true, field: "timestamp"},
		{name: "timestamp equals now", mutate: func(e *RawEvent) { e.Timestamp = now }, wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := validEvent(now)
			tc.mutate(&ev)
			err := ev.Validate(now)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tc.wantErr {
				if !IsValidation(err) {
					t.Fatalf("expected ValidationError, got %T", err)
				}
				if ve := err.(*ValidationError); ve.Field != tc.field {
					t.Fatalf("expected field %q, got %q", tc.field, ve.Field)
				}
			}
		})
	}
}
