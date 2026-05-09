package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

type fakeEvents struct {
	gotEvt domain.ProcessedEvent
	err    error
	calls  int
}

func (f *fakeEvents) SaveEvent(_ context.Context, evt domain.ProcessedEvent) error {
	f.calls++
	f.gotEvt = evt
	return f.err
}

type fakeSummary struct {
	gotEvt domain.ProcessedEvent
	err    error
	calls  int
}

func (f *fakeSummary) UpdateSummary(_ context.Context, evt domain.ProcessedEvent) error {
	f.calls++
	f.gotEvt = evt
	return f.err
}

func validEvent() domain.ProcessedEvent {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	return domain.ProcessedEvent{
		SchemaVersion: 1,
		EventID:       "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID:   "dev-1",
		MetricType:    domain.MetricCommits,
		Value:         3,
		Repository:    "org/repo",
		Timestamp:     now,
		ProcessedAt:   now,
		ProcessorID:   "p1",
	}
}

func TestExecute_RejectsUnknownSchema(t *testing.T) {
	ev, sm := &fakeEvents{}, &fakeSummary{}
	uc := New(ev, sm, time.Now)
	evt := validEvent()
	evt.SchemaVersion = 99
	err := uc.Execute(context.Background(), evt)
	if err == nil {
		t.Fatal("expected error")
	}
	if !domain.IsUnsupportedSchema(err) {
		t.Fatalf("want UnsupportedSchemaError, got %T (%v)", err, err)
	}
	if ev.calls != 0 || sm.calls != 0 {
		t.Errorf("repos must not be called on schema mismatch")
	}
}

func TestExecute_HappyPath_CallsBothInOrder(t *testing.T) {
	ev, sm := &fakeEvents{}, &fakeSummary{}
	uc := New(ev, sm, time.Now)
	if err := uc.Execute(context.Background(), validEvent()); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if ev.calls != 1 {
		t.Errorf("SaveEvent calls = %d, want 1", ev.calls)
	}
	if sm.calls != 1 {
		t.Errorf("UpdateSummary calls = %d, want 1", sm.calls)
	}
	if ev.gotEvt.EventID != sm.gotEvt.EventID {
		t.Errorf("event mismatch between save and summary")
	}
}

func TestExecute_DuplicateEvent_SkipsSummaryAndReturnsNil(t *testing.T) {
	ev := &fakeEvents{err: &domain.AlreadyProcessedError{EventID: "x"}}
	sm := &fakeSummary{}
	uc := New(ev, sm, time.Now)
	if err := uc.Execute(context.Background(), validEvent()); err != nil {
		t.Fatalf("idempotent path must not error, got %v", err)
	}
	if sm.calls != 0 {
		t.Errorf("UpdateSummary must not be called on duplicate, got %d calls", sm.calls)
	}
}

func TestExecute_SaveTransientError_Wrapped_NoSummary(t *testing.T) {
	boom := errors.New("ddb 500")
	ev := &fakeEvents{err: boom}
	sm := &fakeSummary{}
	uc := New(ev, sm, time.Now)
	err := uc.Execute(context.Background(), validEvent())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("want wrapped boom, got %v", err)
	}
	if sm.calls != 0 {
		t.Errorf("UpdateSummary must not be called when SaveEvent failed transiently")
	}
}

func TestExecute_SummaryTransientError_Wrapped(t *testing.T) {
	boom := errors.New("ddb update failed")
	ev := &fakeEvents{}
	sm := &fakeSummary{err: boom}
	uc := New(ev, sm, time.Now)
	err := uc.Execute(context.Background(), validEvent())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("want wrapped boom, got %v", err)
	}
}
