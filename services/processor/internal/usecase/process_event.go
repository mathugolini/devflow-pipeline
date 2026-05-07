package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/domain"
)

// Publisher is the outbound port for the processed-events queue.
type Publisher interface {
	Publish(ctx context.Context, event domain.ProcessedEvent) error
}

// Clock allows tests to inject deterministic time.
type Clock func() time.Time

type ProcessEvent struct {
	publisher   Publisher
	clock       Clock
	processorID string
}

func NewProcessEvent(p Publisher, processorID string, clock Clock) *ProcessEvent {
	if clock == nil {
		clock = time.Now().UTC
	}
	return &ProcessEvent{publisher: p, clock: clock, processorID: processorID}
}

// Execute validates the raw event and publishes the enriched version.
// Returns *domain.ValidationError for permanent rejections; any other
// error is treated as transient (will be retried via SQS visibility timeout).
func (uc *ProcessEvent) Execute(ctx context.Context, raw domain.RawEvent) error {
	now := uc.clock()
	if err := raw.Validate(now); err != nil {
		return err
	}
	processed := domain.ProcessedEvent{
		RawEvent:    raw,
		ProcessedAt: now,
		ProcessorID: uc.processorID,
	}
	if err := uc.publisher.Publish(ctx, processed); err != nil {
		return fmt.Errorf("publish processed event: %w", err)
	}
	return nil
}
