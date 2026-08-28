package main

import (
	"context"
	"fmt"
	"os"

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

	shutdownMgr := shutdown.NewManager(logger, cfg.Worker.LeaseDuration)

	shutdownMgr.Register("PostgreSQL", func(ctx context.Context) error {
		if pgClient != nil {
			pgClient.Close()
		}
		return nil
	})

	shutdownMgr.Register("Redis", func(ctx context.Context) error {
		if rdbClient != nil {
			return rdbClient.Close()
		}
		return nil
	})

	shutdownMgr.Register("WorkerPool", func(ctx context.Context) error {
		logger.Info("draining active worker goroutines")
		return nil
	})

	// Wait for termination signal
	if err := shutdownMgr.Listen(ctx); err != nil {
		logger.Error("worker graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("forgeflow-worker stopped cleanly")
}
