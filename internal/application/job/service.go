package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/application/idempotency"
	"github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	infraRedis "github.com/forgeflow/forgeflow/internal/infrastructure/redis"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// SubmitJobCommand represents the input data to submit a new asynchronous job.
type SubmitJobCommand struct {
	UserID         uuid.UUID       `json:"user_id"`
	QueueID        uuid.UUID       `json:"queue_id"`
	QueueName      string          `json:"queue_name"`
	TaskType       string          `json:"task_type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	ScheduledAt    time.Time       `json:"scheduled_at"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
}

// Service coordinates job use cases across domain, persistence, queue, and idempotency boundaries.
type Service struct {
	jobRepo        ports.JobRepository
	attemptRepo    ports.JobAttemptRepository
	queueEngine    *infraRedis.QueueEngine
	idempotencySvc *idempotency.Service
}

// NewService constructs a new Job application service.
func NewService(jobRepo ports.JobRepository, attemptRepo ports.JobAttemptRepository, queueEngine *infraRedis.QueueEngine, idempotencySvc *idempotency.Service) *Service {
	return &Service{
		jobRepo:        jobRepo,
		attemptRepo:    attemptRepo,
		queueEngine:    queueEngine,
		idempotencySvc: idempotencySvc,
	}
}

// Submit handles idempotent job creation, persistence, outbox event generation, and queue publishing.
// Returns (*job.Job, isDuplicate, cachedResponseBody, error).
func (s *Service) Submit(ctx context.Context, cmd SubmitJobCommand) (*job.Job, bool, []byte, error) {
	payloadBytes := []byte(cmd.Payload)
	if len(payloadBytes) == 0 {
		payloadBytes = []byte("{}")
	}

	// 1. Idempotency Check & Atomic Key Acquisition
	var idemKey string
	if cmd.IdempotencyKey != nil && *cmd.IdempotencyKey != "" {
		idemKey = *cmd.IdempotencyKey
		rec, isNew, err := s.idempotencySvc.CheckOrLock(ctx, cmd.UserID, idemKey, payloadBytes, 24*time.Hour)
		if err != nil {
			return nil, false, nil, err
		}

		// Duplicate request: return cached response if present
		if !isNew && rec != nil && rec.ResponseStatus != nil {
			if rec.ResourceID != nil {
				existingJob, getErr := s.jobRepo.GetByID(ctx, *rec.ResourceID)
				if getErr == nil {
					return existingJob, true, rec.ResponseBody, nil
				}
			}
			return nil, true, rec.ResponseBody, nil
		}
	}

	// 2. Initialize Domain Entity
	j, err := job.NewJob(
		cmd.UserID, cmd.QueueID, cmd.TaskType, payloadBytes,
		cmd.Priority, cmd.MaxAttempts, cmd.ScheduledAt, cmd.TimeoutSeconds, cmd.IdempotencyKey,
	)
	if err != nil {
		return nil, false, nil, err
	}

	// 3. Create Outbox Event
	outboxEvt, err := outbox.NewEvent("job.created", "job", j.ID, map[string]any{
		"job_id":      j.ID,
		"queue_id":    j.QueueID,
		"queue_name":  cmd.QueueName,
		"priority":    j.Priority,
		"task_type":   j.TaskType,
		"scheduled_at": j.ScheduledAt,
	})
	if err != nil {
		return nil, false, nil, fmt.Errorf("failed to create outbox event: %w", err)
	}

	// 4. Persist Job and Outbox Event atomically in PostgreSQL
	if err := s.jobRepo.Create(ctx, j, outboxEvt); err != nil {
		return nil, false, nil, fmt.Errorf("failed to persist job: %w", err)
	}

	// 5. Enqueue to Redis Stream if immediately runnable
	if j.ScheduledAt.Before(time.Now().UTC().Add(100 * time.Millisecond)) {
		queueName := cmd.QueueName
		if queueName == "" {
			queueName = "default"
		}
		if _, enqueueErr := s.queueEngine.Enqueue(ctx, queueName, j.ID, j.Priority); enqueueErr != nil {
			// Non-fatal: outbox reconciler / scheduler will publish the job if Redis is temporarily down
		}
	}

	// 6. Cache Response for Idempotency
	if idemKey != "" {
		respBody, _ := json.Marshal(j)
		resType := "job"
		_ = s.idempotencySvc.SaveResponse(ctx, cmd.UserID, idemKey, 201, respBody, &j.ID, &resType)
	}

	return j, false, nil, nil
}

// GetByID returns job details along with full attempt history.
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*job.Job, []*job.JobAttempt, error) {
	j, err := s.jobRepo.GetByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}

	attempts, err := s.attemptRepo.ListByJobID(ctx, id)
	if err != nil {
		return j, nil, nil // return job even if attempts query fails
	}

	return j, attempts, nil
}

// List returns filtered and paginated jobs.
func (s *Service) List(ctx context.Context, filter ports.JobFilter) ([]*job.Job, int64, error) {
	return s.jobRepo.List(ctx, filter)
}

// Cancel terminates a non-terminal job.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID) error {
	j, err := s.jobRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if j.IsTerminal() {
		return errors.New("cannot cancel job already in terminal state")
	}

	return s.jobRepo.Cancel(ctx, id)
}
