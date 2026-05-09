// Package usecase contains the aggregator's application logic. The
// flow per the challenge spec (step 3.3):
//
//  1. validate schema_version
//  2. SaveEvent — conditional Put on events
//     - on AlreadyProcessed (duplicate event_id), return nil so the
//       worker deletes the SQS message
//  3. UpdateSummary — atomic UpdateItem on developer_summary
package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

// EventRepository persists individual events.
type EventRepository interface {
	// SaveEvent must return *domain.AlreadyProcessedError when the
	// event_id already exists (conditional write failed). Any other
	// error is treated as transient.
	SaveEvent(ctx context.Context, evt domain.ProcessedEvent) error
}

// SummaryRepository updates the materialised per-developer summary.
type SummaryRepository interface {
	UpdateSummary(ctx context.Context, evt domain.ProcessedEvent) error
}

type Clock func() time.Time

type AggregateEvent struct {
	events  EventRepository
	summary SummaryRepository
	clock   Clock
}

func New(events EventRepository, summary SummaryRepository, clock Clock) *AggregateEvent {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &AggregateEvent{events: events, summary: summary, clock: clock}
}

// Execute is the worker-facing entrypoint.
//
// Errors:
//   - *domain.UnsupportedSchemaError → permanent rejection; worker
//     leaves the message for SQS DLQ redrive.
//   - any other non-nil error → treated as transient; worker leaves
//     the message for visibility-timeout-driven retry.
func (uc *AggregateEvent) Execute(ctx context.Context, evt domain.ProcessedEvent) error {
	if evt.SchemaVersion != domain.SupportedSchemaVersion {
		return &domain.UnsupportedSchemaError{Got: evt.SchemaVersion, Want: domain.SupportedSchemaVersion}
	}
	if err := uc.events.SaveEvent(ctx, evt); err != nil {
		if domain.IsAlreadyProcessed(err) {
			// Idempotent retry: event already persisted on a previous
			// delivery. Skip the summary update to avoid double-count
			// and let the worker delete the message.
			return nil
		}
		return fmt.Errorf("save event: %w", err)
	}
	if err := uc.summary.UpdateSummary(ctx, evt); err != nil {
		return fmt.Errorf("update summary: %w", err)
	}
	return nil
}
