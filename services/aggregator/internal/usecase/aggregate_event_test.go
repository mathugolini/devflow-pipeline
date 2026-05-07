package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

type fakeRepo struct {
	gotRec domain.EventRecord
	gotInc domain.SummaryIncrements
	err    error
	calls  int
}

func (f *fakeRepo) PersistAndAggregate(_ context.Context, rec domain.EventRecord, inc domain.SummaryIncrements) error {
	f.calls++
	f.gotRec = rec
	f.gotInc = inc
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
	repo := &fakeRepo{}
	uc := New(repo, time.Now)
	evt := validEvent()
	evt.SchemaVersion = 99
	err := uc.Execute(context.Background(), evt)
	if err == nil {
		t.Fatal("expected error")
	}
	if !domain.IsUnsupportedSchema(err) {
		t.Fatalf("want UnsupportedSchemaError, got %T (%v)", err, err)
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called on schema mismatch")
	}
}

func TestExecute_CommitsIncrement(t *testing.T) {
	repo := &fakeRepo{}
	uc := New(repo, time.Now)
	if err := uc.Execute(context.Background(), validEvent()); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if repo.gotInc.Commits != 3 || repo.gotInc.PullRequests != 0 || repo.gotInc.ReviewTimeMinutes != 0 || repo.gotInc.ReviewTimeCount != 0 {
		t.Errorf("commits inc wrong: %+v", repo.gotInc)
	}
}

func TestExecute_ReviewTimeIncrement(t *testing.T) {
	repo := &fakeRepo{}
	uc := New(repo, time.Now)
	evt := validEvent()
	evt.MetricType = domain.MetricReviewTimeMinutes
	evt.Value = 42.5
	if err := uc.Execute(context.Background(), evt); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if repo.gotInc.ReviewTimeMinutes != 42.5 || repo.gotInc.ReviewTimeCount != 1 {
		t.Errorf("review_time inc wrong: %+v", repo.gotInc)
	}
}

func TestExecute_IdempotentRepoNoError(t *testing.T) {
	// repo returns nil on idempotent duplicate (no AlreadyProcessedError surfaced).
	repo := &fakeRepo{err: nil}
	uc := New(repo, time.Now)
	if err := uc.Execute(context.Background(), validEvent()); err != nil {
		t.Fatalf("idempotent path must not error, got %v", err)
	}
}

func TestExecute_TransientErrorWrapped(t *testing.T) {
	boom := errors.New("ddb 500")
	repo := &fakeRepo{err: boom}
	uc := New(repo, time.Now)
	err := uc.Execute(context.Background(), validEvent())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("want wrapped boom, got %v", err)
	}
	if domain.IsUnsupportedSchema(err) {
		t.Fatal("transient must not be classified as schema mismatch")
	}
}
