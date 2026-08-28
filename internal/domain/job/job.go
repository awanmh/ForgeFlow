package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Domain error definitions.
var (
	ErrInvalidStateTransition = errors.New("invalid job state transition")
	ErrJobNotFound            = errors.New("job not found")
	ErrLeaseExpired           = errors.New("job lease has expired")
	ErrAlreadyClaimed         = errors.New("job is already claimed by another worker")
	ErrMaxAttemptsReached     = errors.New("job max retry attempts reached")
	ErrJobCancelled           = errors.New("job is cancelled")
	ErrInvalidPayload         = errors.New("invalid job payload")
	ErrInvalidPriority        = errors.New("priority must be non-negative")
)

// Status represents the operational lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusRetrying  Status = "RETRYING"
	StatusDead      Status = "DEAD"
	StatusCancelled Status = "CANCELLED"
)

// AttemptStatus represents the execution state of an individual job attempt.
type AttemptStatus string

const (
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptTimeout   AttemptStatus = "TIMEOUT"
	AttemptCancelled AttemptStatus = "CANCELLED"
	AttemptAbandoned AttemptStatus = "ABANDONED"
)

// Job represents the core domain entity for an asynchronous execution task.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	UserID         uuid.UUID       `json:"user_id"`
	WorkflowID     *uuid.UUID      `json:"workflow_id,omitempty"`
	WorkflowNodeID *uuid.UUID      `json:"workflow_node_id,omitempty"`
	QueueID        uuid.UUID       `json:"queue_id"`
	WorkerID       *uuid.UUID      `json:"worker_id,omitempty"`
	TaskType       string          `json:"task_type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Status         Status          `json:"status"`
	AttemptCount   int             `json:"attempt_count"`
	MaxAttempts    int             `json:"max_attempts"`
	ScheduledAt    time.Time       `json:"scheduled_at"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	CancelledAt    *time.Time      `json:"cancelled_at,omitempty"`
	ErrorCode      *string         `json:"error_code,omitempty"`
	ErrorMessage   *string         `json:"error_message,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
}

// JobAttempt represents historical telemetry of an individual job execution attempt.
type JobAttempt struct {
	ID             uuid.UUID       `json:"id"`
	JobID          uuid.UUID       `json:"job_id"`
	AttemptNumber  int             `json:"attempt_number"`
	WorkerID       *uuid.UUID      `json:"worker_id,omitempty"`
	Status         AttemptStatus   `json:"status"`
	StartedAt      time.Time       `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	ErrorCode      *string         `json:"error_code,omitempty"`
	ErrorMessage   *string         `json:"error_message,omitempty"`
	Metadata       json.RawMessage `json:"metadata"`
}

// NewJob initializes a new Job entity in PENDING status.
func NewJob(userID, queueID uuid.UUID, taskType string, payload []byte, priority, maxAttempts int, scheduledAt time.Time, timeoutSec *int, idempotencyKey *string) (*Job, error) {
	if taskType == "" {
		return nil, fmt.Errorf("%w: task_type is required", ErrInvalidPayload)
	}
	if priority < 0 {
		return nil, ErrInvalidPriority
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: payload must be valid JSON", ErrInvalidPayload)
	}
	if scheduledAt.IsZero() {
		scheduledAt = time.Now().UTC()
	}

	now := time.Now().UTC()
	return &Job{
		ID:             uuid.New(),
		UserID:         userID,
		QueueID:        queueID,
		TaskType:       taskType,
		Payload:        payload,
		Priority:       priority,
		Status:         StatusPending,
		AttemptCount:   0,
		MaxAttempts:    maxAttempts,
		ScheduledAt:    scheduledAt,
		TimeoutSeconds: timeoutSec,
		CreatedAt:      now,
		UpdatedAt:      now,
		IdempotencyKey: idempotencyKey,
	}, nil
}

// Enqueue transitions a PENDING or RETRYING job to QUEUED.
func (j *Job) Enqueue(now time.Time) error {
	if j.Status != StatusPending && j.Status != StatusRetrying {
		return fmt.Errorf("%w: cannot transition from %s to QUEUED", ErrInvalidStateTransition, j.Status)
	}
	j.Status = StatusQueued
	j.UpdatedAt = now.UTC()
	return nil
}

// Claim transitions a QUEUED or RETRYING job to RUNNING and acquires a worker lease.
func (j *Job) Claim(workerID uuid.UUID, leaseDuration time.Duration, now time.Time) (*JobAttempt, error) {
	if j.Status != StatusQueued && j.Status != StatusRetrying && j.Status != StatusPending {
		return nil, fmt.Errorf("%w: cannot claim job in status %s", ErrInvalidStateTransition, j.Status)
	}

	nowUTC := now.UTC()
	leaseExpiry := nowUTC.Add(leaseDuration)

	j.Status = StatusRunning
	j.WorkerID = &workerID
	j.AttemptCount++
	j.LeaseExpiresAt = &leaseExpiry
	j.UpdatedAt = nowUTC
	if j.StartedAt == nil {
		j.StartedAt = &nowUTC
	}

	attempt := &JobAttempt{
		ID:             uuid.New(),
		JobID:          j.ID,
		AttemptNumber:  j.AttemptCount,
		WorkerID:       &workerID,
		Status:         AttemptRunning,
		StartedAt:      nowUTC,
		LeaseExpiresAt: &leaseExpiry,
		Metadata:       []byte("{}"),
	}

	return attempt, nil
}

// RenewLease extends the active lease for a RUNNING job.
func (j *Job) RenewLease(workerID uuid.UUID, leaseDuration time.Duration, now time.Time) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("%w: job is not RUNNING (status: %s)", ErrInvalidStateTransition, j.Status)
	}
	if j.WorkerID == nil || *j.WorkerID != workerID {
		return ErrAlreadyClaimed
	}
	if j.LeaseExpiresAt != nil && now.UTC().After(*j.LeaseExpiresAt) {
		return ErrLeaseExpired
	}

	expiry := now.UTC().Add(leaseDuration)
	j.LeaseExpiresAt = &expiry
	j.UpdatedAt = now.UTC()
	return nil
}

// Complete transitions a RUNNING job to SUCCEEDED terminal state.
func (j *Job) Complete(workerID uuid.UUID, now time.Time) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("%w: cannot complete job in status %s", ErrInvalidStateTransition, j.Status)
	}
	if j.WorkerID == nil || *j.WorkerID != workerID {
		return ErrAlreadyClaimed
	}

	nowUTC := now.UTC()
	j.Status = StatusSucceeded
	j.CompletedAt = &nowUTC
	j.LeaseExpiresAt = nil
	j.UpdatedAt = nowUTC
	j.ErrorCode = nil
	j.ErrorMessage = nil
	return nil
}

// Fail records an attempt failure and determines whether to transition to RETRYING or DEAD.
func (j *Job) Fail(workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration, now time.Time) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("%w: cannot fail job in status %s", ErrInvalidStateTransition, j.Status)
	}
	if j.WorkerID == nil || *j.WorkerID != workerID {
		return ErrAlreadyClaimed
	}

	nowUTC := now.UTC()
	j.ErrorCode = &errCode
	j.ErrorMessage = &errMsg
	j.UpdatedAt = nowUTC
	j.LeaseExpiresAt = nil
	j.WorkerID = nil

	if retryable && j.AttemptCount < j.MaxAttempts {
		j.Status = StatusRetrying
		j.ScheduledAt = nowUTC.Add(retryDelay)
	} else {
		j.Status = StatusDead
		j.CompletedAt = &nowUTC
	}

	return nil
}

// MarkAbandoned is called by the recovery scheduler when a worker lease expires.
func (j *Job) MarkAbandoned(retryDelay time.Duration, now time.Time) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("%w: cannot abandon job in status %s", ErrInvalidStateTransition, j.Status)
	}

	nowUTC := now.UTC()
	errCode := "LEASE_TIMEOUT"
	errMsg := "Worker abandoned job or heartbeat expired"
	j.ErrorCode = &errCode
	j.ErrorMessage = &errMsg
	j.UpdatedAt = nowUTC
	j.LeaseExpiresAt = nil
	j.WorkerID = nil

	if j.AttemptCount < j.MaxAttempts {
		j.Status = StatusRetrying
		j.ScheduledAt = nowUTC.Add(retryDelay)
	} else {
		j.Status = StatusDead
		j.CompletedAt = &nowUTC
	}

	return nil
}

// Cancel transitions a cancellable job (PENDING, QUEUED, RETRYING, RUNNING) to CANCELLED.
func (j *Job) Cancel(now time.Time) error {
	switch j.Status {
	case StatusPending, StatusQueued, StatusRetrying, StatusRunning:
		nowUTC := now.UTC()
		j.Status = StatusCancelled
		j.CancelledAt = &nowUTC
		j.LeaseExpiresAt = nil
		j.UpdatedAt = nowUTC
		return nil
	case StatusSucceeded, StatusDead, StatusCancelled:
		return fmt.Errorf("%w: cannot cancel job in terminal status %s", ErrInvalidStateTransition, j.Status)
	default:
		return fmt.Errorf("%w: unhandled state %s", ErrInvalidStateTransition, j.Status)
	}
}

// IsTerminal returns true if the job has reached a permanent terminal status.
func (j *Job) IsTerminal() bool {
	return j.Status == StatusSucceeded || j.Status == StatusDead || j.Status == StatusCancelled
}
