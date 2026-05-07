// Package usecase contains the aggregator's application logic: it
// validates the schema version, derives the persistence record and
// the per-metric increments, and asks the repository to persist
// atomically (events.PutItem + developer_summary.UpdateItem in a
// TransactWriteItems).
package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

// Repository is the outbound port to DynamoDB.
//
// PersistAndAggregate must:
//   - return nil on success
//   - return nil on idempotent duplicate (event_id already exists) so
//     the worker deletes the message
//   - return a non-nil error for any transient failure
type Repository interface {
	PersistAndAggregate(ctx context.Context, rec domain.EventRecord, inc domain.SummaryIncrements) error
}

type Clock func() time.Time

type AggregateEvent struct {
	repo  Repository
	clock Clock
}

func New(r Repository, clock Clock) *AggregateEvent {
	if clock == nil {
		clock = time.Now().UTC
	}
	return &AggregateEvent{repo: r, clock: clock}
}

// Execute is the worker-facing entrypoint.
// On *domain.UnsupportedSchemaError the worker leaves the message for
// SQS DLQ redrive. Any other non-nil error is treated as transient.
func (uc *AggregateEvent) Execute(ctx context.Context, evt domain.ProcessedEvent) error {
	if evt.SchemaVersion != domain.SupportedSchemaVersion {
		return &domain.UnsupportedSchemaError{Got: evt.SchemaVersion, Want: domain.SupportedSchemaVersion}
	}
	rec := evt.ToRecord()
	inc := domain.IncrementsFor(evt, uc.clock())
	if err := uc.repo.PersistAndAggregate(ctx, rec, inc); err != nil {
		return fmt.Errorf("persist and aggregate: %w", err)
	}
	return nil
}
