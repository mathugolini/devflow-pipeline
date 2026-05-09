package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/infra/api"
	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/infra/config"
	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/infra/queue"
	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/infra/repository"
	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/infra/worker"
	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/usecase"
)

const shutdownGrace = 30 * time.Second

func main() {
	cfg, cfgErr := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)
	if cfgErr != nil {
		logger.Error("config load failed", slog.Any("err", cfgErr))
		os.Exit(1)
	}
	if err := run(cfg, logger); err != nil {
		logger.Error("fatal", slog.Any("err", err))
		os.Exit(1)
	}
}

func parseLogLevel(s string) slog.Level {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return lvl
}

// pinger glues SQS + DynamoDB health probes into the api.Pinger port.
type pinger struct {
	consumer *queue.Consumer
	repo     *repository.DynamoDB
}

func (p *pinger) PingSQS(ctx context.Context) error { return p.consumer.Health(ctx) }
func (p *pinger) PingDDB(ctx context.Context) error { return p.repo.HealthEvents(ctx) }

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bootCtx, cancelBoot := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBoot()

	sqsAPI, err := queue.NewClient(bootCtx, cfg.AWSRegion, cfg.AWSEndpointURL)
	if err != nil {
		return fmt.Errorf("sqs client: %w", err)
	}
	queueURL, err := queue.ResolveQueueURL(bootCtx, sqsAPI, cfg.InQueueName)
	if err != nil {
		return err
	}
	consumer := queue.NewConsumer(sqsAPI, cfg.InQueueName, queueURL, cfg.WaitTimeSeconds, 10)

	ddbAPI, err := repository.NewClient(bootCtx, cfg.AWSRegion, cfg.AWSEndpointURL)
	if err != nil {
		return fmt.Errorf("ddb client: %w", err)
	}
	repo := repository.New(ddbAPI, cfg.EventsTable, cfg.SummaryTable)

	// The single DynamoDB struct satisfies both EventRepository and
	// SummaryRepository — wired separately to make the dependencies
	// explicit at the use case boundary.
	uc := usecase.New(repo, repo, nil)
	pool := worker.New(consumer, uc, cfg.Workers, log)

	handler := api.NewHandler(repo, &pinger{consumer: consumer, repo: repo})
	router := api.NewRouter(handler /*, authMiddleware */)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("aggregator started",
		slog.Int("workers", cfg.Workers),
		slog.String("in_queue", cfg.InQueueName),
		slog.String("events_table", cfg.EventsTable),
		slog.String("summary_table", cfg.SummaryTable),
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("log_level", cfg.LogLevel),
	)

	poolDone := make(chan struct{})
	go func() {
		defer close(poolDone)
		pool.Run(ctx)
	}()

	httpDone := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpDone <- err
			return
		}
		httpDone <- nil
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received; draining", slog.Duration("grace", shutdownGrace))
	case err := <-httpDone:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("http shutdown", slog.Any("err", err))
	}
	select {
	case <-poolDone:
		log.Info("aggregator stopped cleanly")
		return nil
	case <-time.After(shutdownGrace):
		log.Error("shutdown grace exceeded; forcing exit")
		os.Exit(2)
		return nil
	}
}
