package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/domain"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/queue"
)

type panickingHandler struct{}

func (panickingHandler) Execute(_ context.Context, _ domain.RawEvent) error {
	panic("boom")
}

// runOne must not propagate a panic from the handler — otherwise a
// single bad message would crash the worker goroutine and the pool
// would silently degrade until it deadlocks.
func TestRunOne_RecoversFromHandlerPanic(t *testing.T) {
	p := &Pool{
		handler: panickingHandler{},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := queue.Message{
		Event:         domain.RawEvent{EventID: "550e8400-e29b-41d4-a716-446655440000"},
		ReceiptHandle: "rh",
		MessageID:     "mid",
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("runOne propagated panic: %v", r)
		}
	}()
	p.runOne(0, msg)
}
