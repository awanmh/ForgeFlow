package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	infraRedis "github.com/forgeflow/forgeflow/internal/infrastructure/redis"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// Config defines execution intervals and thresholds for the scheduler.
type Config struct {
	LeaderLockKey     string
	LeaderLockTTL     time.Duration
	RecoveryInterval  time.Duration
	OutboxInterval    time.Duration
	HeartbeatTimeout  time.Duration
	BatchLimit        int
	DefaultRetryDelay time.Duration
}

// Engine orchestrates lease recovery, dead worker detection, transactional outbox publishing, and leader election.
type Engine struct {
	cfg         Config
	jobRepo     ports.JobRepository
	workerRepo  ports.WorkerRepository
	outboxRepo  ports.OutboxRepository
	queueEngine *infraRedis.QueueEngine
	locker      ports.Locker
	logger      *slog.Logger

	leaderLock ports.Lock
	isLeader   bool
	mu         sync.RWMutex
	stopCh     chan struct{}
}

// NewEngine constructs a new Scheduler Engine instance.
func NewEngine(
	cfg Config,
	jobRepo ports.JobRepository,
	workerRepo ports.WorkerRepository,
	outboxRepo ports.OutboxRepository,
	queueEngine *infraRedis.QueueEngine,
	locker ports.Locker,
	logger *slog.Logger,
) *Engine {
	if cfg.LeaderLockKey == "" {
		cfg.LeaderLockKey = "scheduler-leader"
	}
	if cfg.LeaderLockTTL <= 0 {
		cfg.LeaderLockTTL = 15 * time.Second
	}
	if cfg.RecoveryInterval <= 0 {
		cfg.RecoveryInterval = 5 * time.Second
	}
	if cfg.OutboxInterval <= 0 {
		cfg.OutboxInterval = 1 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 30 * time.Second
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 50
	}
	if cfg.DefaultRetryDelay <= 0 {
		cfg.DefaultRetryDelay = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Engine{
		cfg:         cfg,
		jobRepo:     jobRepo,
		workerRepo:  workerRepo,
		outboxRepo:  outboxRepo,
		queueEngine: queueEngine,
		locker:      locker,
		logger:      logger,
		stopCh:      make(chan struct{}),
	}
}

// Start runs leader election and background periodic recovery loops until context cancellation.
func (e *Engine) Start(ctx context.Context) error {
	e.logger.Info("starting forgeflow scheduler engine",
		"recovery_interval", e.cfg.RecoveryInterval,
		"outbox_interval", e.cfg.OutboxInterval,
	)

	leaderTicker := time.NewTicker(e.cfg.LeaderLockTTL / 3)
	defer leaderTicker.Stop()

	recoveryTicker := time.NewTicker(e.cfg.RecoveryInterval)
	defer recoveryTicker.Stop()

	outboxTicker := time.NewTicker(e.cfg.OutboxInterval)
	defer outboxTicker.Stop()

	// Initial leader attempt
	e.tryAcquireOrRenewLeader(ctx)

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("scheduler context cancelled, stopping engine")
			e.releaseLeader(context.Background())
			return nil
		case <-e.stopCh:
			e.releaseLeader(context.Background())
			return nil

		case <-leaderTicker.C:
			e.tryAcquireOrRenewLeader(ctx)

		case <-recoveryTicker.C:
			if e.IsLeader() {
				e.runLeaseRecovery(ctx)
				e.runDeadWorkerSweep(ctx)
			}

		case <-outboxTicker.C:
			if e.IsLeader() {
				e.runOutboxPublisher(ctx)
			}
		}
	}
}

func (e *Engine) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *Engine) setLeader(val bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLeader = val
}

// tryAcquireOrRenewLeader maintains distributed leader election.
func (e *Engine) tryAcquireOrRenewLeader(ctx context.Context) {
	if e.locker == nil {
		// Single instance standalone mode
		e.setLeader(true)
		return
	}

	if e.IsLeader() && e.leaderLock != nil {
		if err := e.leaderLock.Renew(ctx, e.cfg.LeaderLockTTL); err != nil {
			e.logger.Warn("lost leader lease during renewal", "error", err)
			e.setLeader(false)
			e.leaderLock = nil
		}
		return
	}

	lock, err := e.locker.Acquire(ctx, e.cfg.LeaderLockKey, e.cfg.LeaderLockTTL)
	if err != nil {
		e.setLeader(false)
		return
	}

	e.logger.Info("acquired scheduler leader lease", "token", lock.Token())
	e.leaderLock = lock
	e.setLeader(true)
}

func (e *Engine) releaseLeader(ctx context.Context) {
	if e.leaderLock != nil {
		_ = e.leaderLock.Release(ctx)
		e.leaderLock = nil
	}
	e.setLeader(false)
}

// runLeaseRecovery sweeps for expired worker leases and re-enqueues or fails them.
func (e *Engine) runLeaseRecovery(ctx context.Context) {
	if e.jobRepo == nil {
		return
	}
	recovered, err := e.jobRepo.RecoverExpiredLeases(ctx, e.cfg.BatchLimit, e.cfg.DefaultRetryDelay)
	if err != nil {
		e.logger.Error("lease recovery sweep failed", "error", err)
		return
	}

	if len(recovered) > 0 {
		e.logger.Warn("recovered expired job leases", "count", len(recovered))
	}
}

// runDeadWorkerSweep marks inactive workers as DEAD.
func (e *Engine) runDeadWorkerSweep(ctx context.Context) {
	if e.workerRepo == nil {
		return
	}
	deadWorkers, err := e.workerRepo.FindDeadWorkers(ctx, e.cfg.HeartbeatTimeout)
	if err != nil {
		e.logger.Error("dead worker sweep failed", "error", err)
		return
	}

	for _, w := range deadWorkers {
		e.logger.Warn("marking worker dead due to missed heartbeats",
			"worker_id", w.ID,
			"worker_key", w.WorkerKey,
			"last_heartbeat_at", w.LastHeartbeatAt,
		)
		_ = e.workerRepo.UpdateStatus(ctx, w.ID, "DEAD")
	}
}

// runOutboxPublisher publishes pending transactional outbox events to Redis Streams.
func (e *Engine) runOutboxPublisher(ctx context.Context) {
	if e.outboxRepo == nil {
		return
	}
	events, err := e.outboxRepo.FetchPending(ctx, e.cfg.BatchLimit)
	if err != nil {
		e.logger.Error("failed to fetch pending outbox events", "error", err)
		return
	}

	for _, evt := range events {
		if e.queueEngine != nil && evt.AggregateType == "job" {
			// Enqueue job to Redis Stream
			jobID := evt.AggregateID
			if jobID != uuid.Nil {
				if _, err := e.queueEngine.Enqueue(ctx, "default", jobID, 0); err != nil {
					_ = e.outboxRepo.MarkFailed(ctx, evt.ID)
					continue
				}
			}
		}

		if err := e.outboxRepo.MarkPublished(ctx, evt.ID); err != nil {
			e.logger.Error("failed to mark outbox event published", "event_id", evt.ID, "error", err)
		}
	}
}

// Stop signals the scheduler to stop running.
func (e *Engine) Stop() {
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
}
