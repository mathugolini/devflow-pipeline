// Package worker hosts the long-poll dispatcher and the worker pool
// that consumes processed-events. Mirrors the processor's pool: on
// permanent errors (UnsupportedSchemaError) the message is left on
// the queue so SQS can DLQ it after maxReceiveCount.
package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/infra/queue"
)

type Handler interface {
	Execute(ctx context.Context, evt domain.ProcessedEvent) error
}

const (
	handlerTimeout        = 25 * time.Second
	receiveBackoffInitial = time.Second
	receiveBackoffMax     = 30 * time.Second
)

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

func (p *Pool) Run(ctx context.Context) {
	jobs := make(chan queue.Message)
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for msg := range jobs {
				p.runOne(id, msg)
			}
		}(i)
	}
	p.dispatch(ctx, jobs)
	close(jobs)
	wg.Wait()
}

func (p *Pool) dispatch(ctx context.Context, jobs chan<- queue.Message) {
	backoff := receiveBackoffInitial
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, parseFails, err := p.consumer.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			wait := backoff + jitter(backoff)
			p.log.Error("receive failed; backing off",
				slog.Any("err", err), slog.Duration("wait", wait))
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return
			case <-t.C:
			}
			backoff *= 2
			if backoff > receiveBackoffMax {
				backoff = receiveBackoffMax
			}
			continue
		}
		backoff = receiveBackoffInitial
		for _, m := range parseFails {
			id := ""
			if m.MessageId != nil {
				id = *m.MessageId
			}
			p.log.Warn("malformed message left for SQS redrive", slog.String("message_id", id))
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

func (p *Pool) runOne(id int, msg queue.Message) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("worker panic recovered",
				slog.Any("panic", r),
				slog.String("event_id", msg.Event.EventID),
				slog.String("message_id", msg.MessageID),
				slog.Int("worker", id),
				slog.String("stack", string(debug.Stack())),
			)
		}
	}()
	hctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	p.handle(hctx, id, msg)
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
			logger.Error("delete after success failed", slog.Any("err", delErr))
			return
		}
		logger.Info("aggregated")
	case domain.IsUnsupportedSchema(err):
		// Permanent rejection: leave for SQS DLQ redrive.
		logger.Warn("unsupported schema; leaving for DLQ redrive", slog.Any("err", err))
	default:
		logger.Error("transient failure; leaving for retry", slog.Any("err", err))
	}
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(d) / 2))
}
