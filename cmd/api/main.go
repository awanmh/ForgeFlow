package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	appAuth "github.com/forgeflow/forgeflow/internal/application/auth"
	"github.com/forgeflow/forgeflow/internal/application/idempotency"
	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	appWorkflow "github.com/forgeflow/forgeflow/internal/application/workflow"
	infraAuth "github.com/forgeflow/forgeflow/internal/infrastructure/auth"
	"github.com/forgeflow/forgeflow/internal/infrastructure/config"
	"github.com/forgeflow/forgeflow/internal/infrastructure/logging"
	"github.com/forgeflow/forgeflow/internal/infrastructure/postgres"
	"github.com/forgeflow/forgeflow/internal/infrastructure/redis"
	"github.com/forgeflow/forgeflow/internal/infrastructure/shutdown"
	httpInterface "github.com/forgeflow/forgeflow/internal/interfaces/http"
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

	logger.Info("starting forgeflow-api",
		"env", cfg.Environment,
		"port", cfg.Server.Port,
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

	// Initialize Security and Rate Limiting
	jwtManager := infraAuth.NewJWTManager(cfg.Auth.JWTSecret, cfg.Auth.AccessTokenTTL)
	rateLimiter := httpInterface.NewRateLimiter(rdbClient, 100, 60*time.Second)

	// Initialize Repositories and Services
	userRepo := postgres.NewUserRepo(pgClient)
	jobRepo := postgres.NewJobRepo(pgClient)
	attemptRepo := postgres.NewJobAttemptRepo(pgClient)
	queueRepo := postgres.NewQueueRepo(pgClient)
	wfRepo := postgres.NewWorkflowRepo(pgClient)
	idempotencyRepo := postgres.NewIdempotencyRepo(pgClient)
	idempotencySvc := idempotency.NewService(idempotencyRepo)
	queueEngine := redis.NewQueueEngine(rdbClient, "forgeflow-workers")

	authSvc := appAuth.NewService(userRepo, jwtManager)
	jobSvc := appJob.NewService(jobRepo, attemptRepo, queueEngine, idempotencySvc)
	wfSvc := appWorkflow.NewService(wfRepo, jobSvc, queueRepo, jobRepo)

	authHandler := httpInterface.NewAuthHandler(authSvc)
	jobHandler := httpInterface.NewJobHandler(jobSvc, queueRepo)
	wfHandler := httpInterface.NewWorkflowHandler(wfSvc)

	// Initialize Router
	router := httpInterface.NewRouter(pgClient, rdbClient, logger, httpInterface.RouterOptions{
		JWTManager:      jwtManager,
		RateLimiter:     rateLimiter,
		AuthHandler:     authHandler,
		JobHandler:      jobHandler,
		WorkflowHandler: wfHandler,
	})

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      router.Engine,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: 0, // Disabled to support persistent Server-Sent Events (SSE) streaming
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	shutdownMgr := shutdown.NewManager(logger, cfg.Worker.LeaseDuration)

	// Register LIFO cleanup hooks
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

	shutdownMgr.Register("HTTPServer", func(ctx context.Context) error {
		return httpServer.Shutdown(ctx)
	})

	// Start HTTP Server in background goroutine
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for termination signal
	if err := shutdownMgr.Listen(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("forgeflow-api stopped cleanly")
}
