package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/domain"
)

type fakePublisher struct {
	got domain.ProcessedEvent
	err error
}

func (f *fakePublisher) Publish(_ context.Context, e domain.ProcessedEvent) error {
	f.got = e
	return f.err
}

func TestProcessEvent_Execute_Success(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	pub := &fakePublisher{}
	uc := NewProcessEvent(pub, "processor-test", func() time.Time { return now })

	raw := domain.RawEvent{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  domain.MetricCommits,
		Value:       2,
		Repository:  "org/repo",
		Timestamp:   now.Add(-time.Minute),
	}

	if err := uc.Execute(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub.got.ProcessorID != "processor-test" {
		t.Errorf("processor_id not set, got %q", pub.got.ProcessorID)
	}
	if !pub.got.ProcessedAt.Equal(now) {
		t.Errorf("processed_at not set to clock value")
	}
	if pub.got.EventID != raw.EventID {
		t.Errorf("payload not preserved")
	}
}

func TestProcessEvent_Execute_ValidationError(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	pub := &fakePublisher{}
	uc := NewProcessEvent(pub, "p", func() time.Time { return now })

	err := uc.Execute(context.Background(), domain.RawEvent{}) // empty -> invalid
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !domain.IsValidation(err) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if (pub.got != domain.ProcessedEvent{}) {
		t.Error("publisher should not be called on validation error")
	}
}

func TestProcessEvent_Execute_PublishFailureIsTransient(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	boom := errors.New("sqs unavailable")
	pub := &fakePublisher{err: boom}
	uc := NewProcessEvent(pub, "p", func() time.Time { return now })

	raw := domain.RawEvent{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  domain.MetricCommits,
		Value:       1,
		Timestamp:   now,
	}

	err := uc.Execute(context.Background(), raw)
	if err == nil {
		t.Fatal("expected error")
	}
	if domain.IsValidation(err) {
		t.Fatal("publish failure must not be classified as validation")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped publisher error, got %v", err)
	}
}
