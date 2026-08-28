package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/application/task"
	"github.com/forgeflow/forgeflow/internal/infrastructure/logging"
	infraRedis "github.com/forgeflow/forgeflow/internal/infrastructure/redis"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// Config represents runtime configuration parameters for the worker engine.
type Config struct {
	WorkerID          uuid.UUID
	WorkerName        string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	DefaultTimeout    time.Duration
	MaxRetries        int
	BackoffInitial    time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64
	JitterPercent     float64
	QueueName         string
}

// Engine coordinates queue consumption, bounded concurrency execution, leases, and failure recovery.
type Engine struct {
	cfg         Config
	jobRepo     ports.JobRepository
	attemptRepo ports.JobAttemptRepository
	queue       *infraRedis.QueueEngine
	registry    *task.Registry
	logger      *slog.Logger

	sem    chan struct{}
	wg     sync.WaitGroup
	stopCh chan struct{}
}

// NewEngine initializes a new Worker Engine instance.
func NewEngine(cfg Config, jobRepo ports.JobRepository, attemptRepo ports.JobAttemptRepository, queue *infraRedis.QueueEngine, registry *task.Registry, logger *slog.Logger) *Engine {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 60 * time.Second
	}
	if cfg.BackoffInitial <= 0 {
		cfg.BackoffInitial = 1 * time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 60 * time.Second
	}
	if cfg.BackoffMultiplier <= 1.0 {
		cfg.BackoffMultiplier = 2.0
	}
	if cfg.QueueName == "" {
		cfg.QueueName = "default"
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Engine{
		cfg:      cfg,
		jobRepo:  jobRepo,
		queue:    queue,
		registry: registry,
		logger:   logger,
		sem:      make(chan struct{}, cfg.Concurrency),
		stopCh:   make(chan struct{}),
	}
}

// Start initiates the queue consumer loop until the context is cancelled.
func (e *Engine) Start(ctx context.Context) error {
	e.logger.Info("starting worker engine",
		"worker_id", e.cfg.WorkerID,
		"worker_name", e.cfg.WorkerName,
		"concurrency", e.cfg.Concurrency,
		"queue", e.cfg.QueueName,
		"lease_duration", e.cfg.LeaseDuration,
	)

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("worker context cancelled, stopping consumer loop")
			return e.Drain(context.Background())
		case <-e.stopCh:
			return e.Drain(context.Background())
		case <-ticker.C:
			// Attempt to acquire an execution slot from the semaphore non-blockingly
			select {
			case e.sem <- struct{}{}:
				// Acquired slot: dequeue job from Redis Stream
				messages, err := e.queue.Dequeue(ctx, e.cfg.QueueName, e.cfg.WorkerName, 1, 100*time.Millisecond)
				if err != nil {
					e.logger.Debug("queue dequeue empty or failed", "error", err)
					<-e.sem // release slot
					continue
				}

				if len(messages) == 0 {
					<-e.sem // release slot
					continue
				}

				msg := messages[0]
				e.wg.Add(1)
				go func(m infraRedis.QueueMessage) {
					defer func() {
						<-e.sem
						e.wg.Done()
					}()
					e.processJob(ctx, m)
				}(msg)

			default:
				// Worker pool is saturated at max concurrency; wait for next tick
			}
		}
	}
}

// processJob executes a single claimed job with timeout, heartbeats, and panic protection.
func (e *Engine) processJob(ctx context.Context, msg infraRedis.QueueMessage) {
	jobLogger := e.logger.With(
		"job_id", msg.JobID,
		"queue_msg_id", msg.ID,
		"worker_id", e.cfg.WorkerID,
	)

	// Step 1: Claim job in PostgreSQL
	j, err := e.jobRepo.GetByID(ctx, msg.JobID)
	if err != nil {
		jobLogger.Warn("job not found in PostgreSQL during processing", "error", err)
		_ = e.queue.Ack(ctx, e.cfg.QueueName, msg.ID)
		return
	}

	if j.IsTerminal() {
		jobLogger.Info("job is already in terminal state, skipping execution", "status", j.Status)
		_ = e.queue.Ack(ctx, e.cfg.QueueName, msg.ID)
		return
	}

	// Claim lease
	now := time.Now().UTC()
	att, err := j.Claim(e.cfg.WorkerID, e.cfg.LeaseDuration, now)
	if err != nil {
		jobLogger.Warn("failed to claim job in domain state machine", "error", err)
		_ = e.queue.Ack(ctx, e.cfg.QueueName, msg.ID)
		return
	}

	// Record attempt in database
	if e.attemptRepo != nil {
		if err := e.attemptRepo.Create(ctx, att); err != nil {
			jobLogger.Warn("failed to persist initial job attempt", "error", err)
		}
	}

	// Step 2: Start background heartbeat renewal
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()

	timeout := e.cfg.DefaultTimeout
	if j.TimeoutSeconds != nil && *j.TimeoutSeconds > 0 {
		timeout = time.Duration(*j.TimeoutSeconds) * time.Second
	}
	execCtx, cancelExec := context.WithTimeout(jobCtx, timeout)
	defer cancelExec()

	// Correlate logger context
	execCtx = logging.WithJobID(execCtx, j.ID.String())
	execCtx = logging.WithWorkerID(execCtx, e.cfg.WorkerID.String())

	// Heartbeat routine
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		hbTicker := time.NewTicker(e.cfg.HeartbeatInterval)
		defer hbTicker.Stop()

		for {
			select {
			case <-jobCtx.Done():
				return
			case <-hbTicker.C:
				if hbErr := e.jobRepo.RenewLease(jobCtx, j.ID, e.cfg.WorkerID, e.cfg.LeaseDuration); hbErr != nil {
					jobLogger.Warn("failed to renew job lease heartbeat", "error", hbErr)
					cancelExec() // Cancel execution if lease ownership is lost
					return
				}
				jobLogger.Debug("job lease heartbeat renewed successfully")
			}
		}
	}()

	// Step 3: Execute task from registry with panic recovery
	execErr := e.executeTask(execCtx, j.TaskType, j.Payload)

	// Stop heartbeats
	cancelJob()
	<-heartbeatDone

	// Step 4: Record outcome and transition state
	if execErr == nil {
		jobLogger.Info("job task executed successfully", "attempt", j.AttemptCount)
		if compErr := e.jobRepo.Complete(ctx, j.ID, e.cfg.WorkerID); compErr != nil {
			jobLogger.Error("failed to mark job completed in postgres", "error", compErr)
		}
	} else {
		jobLogger.Error("job task execution failed",
			"error", execErr,
			"attempt", j.AttemptCount,
			"max_attempts", j.MaxAttempts,
		)

		retryDelay := e.CalculateBackoff(j.AttemptCount)
		errCode := "TASK_EXECUTION_ERROR"
		errMsg := execErr.Error()

		if failErr := e.jobRepo.Fail(ctx, j.ID, e.cfg.WorkerID, errCode, errMsg, true, retryDelay); failErr != nil {
			jobLogger.Error("failed to record job failure in postgres", "error", failErr)
		}
	}

	// Step 5: ACK message in Redis Streams
	if ackErr := e.queue.Ack(ctx, e.cfg.QueueName, msg.ID); ackErr != nil {
		jobLogger.Warn("failed to XACK message in redis stream", "error", ackErr)
	}
}

// executeTask handles task invocation with panic recovery.
func (e *Engine) executeTask(ctx context.Context, taskType string, payload []byte) (execErr error) {
	defer func() {
		if r := recover(); r != nil {
			execErr = fmt.Errorf("task panicked during execution: %v", r)
		}
	}()

	handler, ok := e.registry.Get(taskType)
	if !ok {
		return fmt.Errorf("%w: %s", task.ErrTaskTypeNotFound, taskType)
	}

	return handler.Execute(ctx, payload)
}

// CalculateBackoff calculates exponential backoff duration with full jitter to avoid thundering herds.
func (e *Engine) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// base = initial * multiplier^(attempt-1)
	base := float64(e.cfg.BackoffInitial) * math.Pow(e.cfg.BackoffMultiplier, float64(attempt-1))
	max := float64(e.cfg.BackoffMax)
	if base > max {
		base = max
	}

	// Apply jitter: delay +/- jitterPercent
	jitterRange := base * e.cfg.JitterPercent
	jitter := (rand.Float64()*2 - 1) * jitterRange // [-jitterRange, +jitterRange]
	actual := time.Duration(base + jitter)
	if actual < 100*time.Millisecond {
		actual = 100 * time.Millisecond
	}

	return actual
}

// Drain waits for active in-flight worker tasks to finish before shutting down.
func (e *Engine) Drain(ctx context.Context) error {
	e.logger.Info("draining worker engine tasks")
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.logger.Info("all in-flight worker tasks drained cleanly")
		return nil
	case <-ctx.Done():
		return errors.New("worker drain timeout exceeded")
	}
}

// Stop signals the worker engine to stop consuming and shut down.
func (e *Engine) Stop() {
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
}
