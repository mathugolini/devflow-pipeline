package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/config"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/queue"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/infra/worker"
	"github.com/mathugolini/devflow-pipeline/services/processor/internal/usecase"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bootCtx, cancelBoot := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBoot()

	client, err := queue.NewClient(bootCtx, cfg.AWSRegion, cfg.AWSEndpointURL)
	if err != nil {
		return err
	}
	if err := client.Ping(bootCtx); err != nil {
		return err
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
	)

	pool.Run(ctx)

	log.Info("processor stopped")
	return nil
}
