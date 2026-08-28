package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/application/task"
	"github.com/forgeflow/forgeflow/internal/application/worker"
	"github.com/forgeflow/forgeflow/internal/infrastructure/config"
	"github.com/forgeflow/forgeflow/internal/infrastructure/logging"
	"github.com/forgeflow/forgeflow/internal/infrastructure/postgres"
	"github.com/forgeflow/forgeflow/internal/infrastructure/redis"
	"github.com/forgeflow/forgeflow/internal/infrastructure/shutdown"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	logger := logging.New(logging.Config{
		Level: cfg.LogLevel,
	})

	logger.Info("starting forgeflow-worker",
		"worker_id", cfg.Worker.WorkerID,
		"concurrency", cfg.Worker.Concurrency,
		"lease_duration", cfg.Worker.LeaseDuration,
		"heartbeat_interval", cfg.Worker.HeartbeatInterval,
	)

	ctx := context.Background()

	// Initialize Postgres Client
	pgClient, err := postgres.NewClient(ctx, cfg.Database, logger)
	if err != nil {
		logger.Warn("postgres connection failed during startup (will retry on demand)", "error", err)
	}

	// Initialize Redis Client
	rdbClient, err := redis.NewClient(ctx, cfg.Redis, logger)
	if err != nil {
		logger.Warn("redis connection failed during startup (will retry on demand)", "error", err)
	}

	jobRepo := postgres.NewJobRepo(pgClient)
	attemptRepo := postgres.NewJobAttemptRepo(pgClient)
	queueEngine := redis.NewQueueEngine(rdbClient, "forgeflow-workers")
	taskRegistry := task.NewRegistry()

	workerUUID := uuid.New()
	workerEngine := worker.NewEngine(worker.Config{
		WorkerID:          workerUUID,
		WorkerName:        cfg.Worker.WorkerID,
		Concurrency:       cfg.Worker.Concurrency,
		PollInterval:      cfg.Worker.PollInterval,
		LeaseDuration:     cfg.Worker.LeaseDuration,
		HeartbeatInterval: cfg.Worker.HeartbeatInterval,
		DefaultTimeout:    cfg.Worker.DefaultTimeout,
		MaxRetries:        cfg.Worker.MaxRetries,
		BackoffInitial:    cfg.Worker.BackoffInitial,
		BackoffMax:        cfg.Worker.BackoffMax,
		BackoffMultiplier: cfg.Worker.BackoffMultiplier,
		JitterPercent:     cfg.Worker.JitterPercent,
		QueueName:         "default",
	}, jobRepo, attemptRepo, queueEngine, taskRegistry, logger)

	workerCtx, cancelWorker := context.WithCancel(ctx)

	// Start worker loop in background
	go func() {
		if err := workerEngine.Start(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker engine error", "error", err)
		}
	}()

	shutdownMgr := shutdown.NewManager(logger, cfg.Worker.LeaseDuration)

	shutdownMgr.Register("WorkerPool", func(ctx context.Context) error {
		logger.Info("stopping worker consumer loop and draining active tasks")
		cancelWorker()
		workerEngine.Stop()
		return workerEngine.Drain(ctx)
	})

	shutdownMgr.Register("Redis", func(ctx context.Context) error {
		if rdbClient != nil {
			return rdbClient.Close()
		}
		return nil
	})

	shutdownMgr.Register("PostgreSQL", func(ctx context.Context) error {
		if pgClient != nil {
			pgClient.Close()
		}
		return nil
	})

	// Wait for termination signal
	if err := shutdownMgr.Listen(ctx); err != nil {
		logger.Error("worker graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("forgeflow-worker stopped cleanly")
}
