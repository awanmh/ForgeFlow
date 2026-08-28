package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/application/task"
	"github.com/forgeflow/forgeflow/internal/application/worker"
	domainWorker "github.com/forgeflow/forgeflow/internal/domain/worker"
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
	workerRepo := postgres.NewWorkerRepo(pgClient)
	queueEngine := redis.NewQueueEngine(rdbClient, "forgeflow-workers")
	taskRegistry := task.NewRegistry()

	workerUUID := uuid.New()
	hostname, _ := os.Hostname()
	now := time.Now().UTC()

	// Register worker entity in PostgreSQL to satisfy foreign key constraints
	workerEntity := &domainWorker.Worker{
		ID:              workerUUID,
		WorkerKey:       cfg.Worker.WorkerID,
		Hostname:        hostname,
		Version:         "1.0.0",
		Status:          domainWorker.StatusActive,
		Concurrency:     cfg.Worker.Concurrency,
		RegisteredAt:    now,
		StartedAt:       &now,
		LastHeartbeatAt: &now,
	}
	if regErr := workerRepo.Register(ctx, workerEntity); regErr != nil {
		logger.Warn("failed to register worker in postgres during startup", "error", regErr)
	}

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
		cancelWorker()
		workerEngine.Stop()
		_ = workerRepo.UpdateStatus(ctx, workerUUID, domainWorker.StatusStopped)
		return nil
	})

	shutdownMgr.Register("RedisClient", func(ctx context.Context) error {
		return rdbClient.Close()
	})

	shutdownMgr.Register("PostgresPool", func(ctx context.Context) error {
		pgClient.Close()
		return nil
	})

	// Block awaiting SIGINT / SIGTERM
	if err := shutdownMgr.Listen(ctx); err != nil {
		logger.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}

	logger.Info("forgeflow-worker shutdown gracefully")
}
