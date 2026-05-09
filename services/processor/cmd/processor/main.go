package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/config"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/queue"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/tracing"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/worker"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/usecase"
)

// version is stamped on every span via the OTel resource attributes.
// Bumping it on releases makes it cheap to spot a bad rollout in the
// trace UI. Static for now; CI can override via -ldflags="-X main.version=...".
var version = "dev"

// Hard cap on how long we wait for in-flight work to drain after a
// SIGTERM/SIGINT before forcing exit. Must be larger than the maximum
// in-flight handler timeout so we never exit while a handler is still
// processing — but small enough that orchestrators don't SIGKILL us.
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

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bootCtx, cancelBoot := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBoot()

	shutdownTracing, err := tracing.Init(bootCtx, "devflow-processor", version)
	if err != nil {
		return fmt.Errorf("tracing init: %w", err)
	}
	defer func() {
		// Best-effort flush at shutdown. Bounded so a slow collector
		// cannot stall the container's exit.
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			log.Warn("tracing shutdown error", slog.Any("err", err))
		}
	}()

	client, err := queue.NewClient(bootCtx, cfg.AWSRegion, cfg.AWSEndpointURL)
	if err != nil {
		return fmt.Errorf("aws client: %w", err)
	}
	rawURL, err := client.QueueURL(bootCtx, cfg.RawQueueName)
	if err != nil {
		return err
	}
	outURL, err := client.QueueURL(bootCtx, cfg.OutQueueName)
	if err != nil {
		return err
	}

	consumer := queue.NewConsumer(client, rawURL, cfg.WaitTimeSeconds, 10)
	publisher := queue.NewPublisher(client, outURL)
	uc := usecase.NewProcessEvent(publisher, cfg.ProcessorID, nil)

	pool := worker.New(consumer, uc, cfg.Workers, log)

	log.Info("processor started",
		slog.String("processor_id", cfg.ProcessorID),
		slog.Int("workers", cfg.Workers),
		slog.String("raw_queue", cfg.RawQueueName),
		slog.String("out_queue", cfg.OutQueueName),
		slog.String("log_level", cfg.LogLevel),
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Run(ctx)
	}()

	<-ctx.Done()
	log.Info("shutdown signal received; draining workers", slog.Duration("grace", shutdownGrace))

	select {
	case <-done:
		log.Info("processor stopped cleanly")
		return nil
	case <-time.After(shutdownGrace):
		log.Error("shutdown grace exceeded; forcing exit")
		os.Exit(2)
		return nil
	}
}
