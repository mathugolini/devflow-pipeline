// Package worker hosts the long-poll dispatcher and the worker pool
// that processes raw events concurrently.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/domain"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/queue"
)

// Handler is the inbound port: takes one raw event and returns nil
// on success, *domain.ValidationError for permanent rejections, or
// any other error for transient failures (will be retried by SQS).
type Handler interface {
	Execute(ctx context.Context, raw domain.RawEvent) error
}

type Pool struct {
	consumer *queue.Consumer
	handler  Handler
	workers  int
	log      *slog.Logger
}

func New(c *queue.Consumer, h Handler, workers int, log *slog.Logger) *Pool {
	if workers < 1 {
		workers = 1
	}
	return &Pool{consumer: c, handler: h, workers: workers, log: log}
}

// Run blocks until ctx is cancelled. On cancellation it stops accepting
// new messages and waits for in-flight ones to finish (graceful shutdown).
func (p *Pool) Run(ctx context.Context) {
	jobs := make(chan queue.Message)
	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.workerLoop(ctx, id, jobs)
		}(i)
	}

	p.dispatch(ctx, jobs)
	close(jobs)
	wg.Wait()
}

// dispatch long-polls SQS and feeds messages into the jobs channel until
// ctx is cancelled.
func (p *Pool) dispatch(ctx context.Context, jobs chan<- queue.Message) {
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, parseFails, err := p.consumer.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("receive failed", slog.String("err", err.Error()))
			continue
		}
		for _, m := range parseFails {
			id := ""
			if m.MessageId != nil {
				id = *m.MessageId
			}
			p.log.Warn("malformed message left for SQS redrive",
				slog.String("message_id", id))
		}
		for _, m := range msgs {
			select {
			case <-ctx.Done():
				return
			case jobs <- m:
			}
		}
	}
}

func (p *Pool) workerLoop(ctx context.Context, id int, jobs <-chan queue.Message) {
	for msg := range jobs {
		// Use a detached background context so an in-flight message can
		// finish even after the parent ctx is cancelled (graceful drain).
		// We still respect cancellation upstream via the dispatcher.
		p.handle(context.Background(), id, msg)
	}
}

func (p *Pool) handle(ctx context.Context, workerID int, msg queue.Message) {
	logger := p.log.With(
		slog.String("event_id", msg.Event.EventID),
		slog.String("message_id", msg.MessageID),
		slog.Int("worker", workerID),
	)

	err := p.handler.Execute(ctx, msg.Event)
	switch {
	case err == nil:
		if delErr := p.consumer.Delete(ctx, msg.ReceiptHandle); delErr != nil {
			logger.Error("delete after success failed", slog.String("err", delErr.Error()))
			return
		}
		logger.Info("processed")
	case isValidation(err):
		// Permanent rejection: do NOT delete. SQS will redrive to DLQ
		// after maxReceiveCount.
		logger.Warn("validation failed; leaving for DLQ redrive",
			slog.String("err", err.Error()))
	default:
		// Transient failure: do NOT delete. SQS visibility timeout will
		// re-deliver. After maxReceiveCount the DLQ catches it.
		logger.Error("transient failure; leaving for retry",
			slog.String("err", err.Error()))
	}
}

func isValidation(err error) bool {
	var ve *domain.ValidationError
	return errors.As(err, &ve)
}
