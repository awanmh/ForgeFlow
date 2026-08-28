package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// JobRepo implements ports.JobRepository and ports.JobAttemptRepository via PostgreSQL.
type JobRepo struct {
	client *Client
}

// NewJobRepo constructs a new JobRepo instance.
func NewJobRepo(client *Client) *JobRepo {
	return &JobRepo{client: client}
}

// Create inserts a job and an optional outbox event atomically within a transaction.
func (r *JobRepo) Create(ctx context.Context, j *job.Job, event *outbox.Event) error {
	return r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		query := `
			INSERT INTO jobs (
				id, user_id, workflow_id, workflow_node_id, queue_id,
				task_type, payload, priority, status, attempt_count,
				max_attempts, scheduled_at, timeout_seconds, idempotency_key,
				created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5,
				$6, $7, $8, $9, $10,
				$11, $12, $13, $14,
				$15, $16
			)
		`
		_, err := tx.Exec(ctx, query,
			j.ID, j.UserID, j.WorkflowID, j.WorkflowNodeID, j.QueueID,
			j.TaskType, j.Payload, j.Priority, string(j.Status), j.AttemptCount,
			j.MaxAttempts, j.ScheduledAt, j.TimeoutSeconds, j.IdempotencyKey,
			j.CreatedAt, j.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert job: %w", err)
		}

		if event != nil {
			outboxQuery := `
				INSERT INTO outbox_events (
					id, event_type, aggregate_type, aggregate_id,
					payload, status, attempts, available_at, created_at
				) VALUES (
					$1, $2, $3, $4,
					$5, $6, $7, $8, $9
				)
			`
			_, err = tx.Exec(ctx, outboxQuery,
				event.ID, event.EventType, event.AggregateType, event.AggregateID,
				event.Payload, string(event.Status), event.Attempts, event.AvailableAt, event.CreatedAt,
			)
			if err != nil {
				return fmt.Errorf("failed to insert outbox event for job: %w", err)
			}
		}

		return nil
	})
}

// GetByID fetches a job by UUID.
func (r *JobRepo) GetByID(ctx context.Context, id uuid.UUID) (*job.Job, error) {
	query := `
		SELECT
			id, user_id, workflow_id, workflow_node_id, queue_id, worker_id,
			task_type, payload, priority, status, attempt_count, max_attempts,
			scheduled_at, timeout_seconds, lease_expires_at, created_at, updated_at,
			started_at, completed_at, cancelled_at, error_code, error_message, idempotency_key
		FROM jobs
		WHERE id = $1
	`
	row := r.client.Pool.QueryRow(ctx, query, id)
	return scanJob(row)
}

// List returns filtered and paginated jobs with total count.
func (r *JobRepo) List(ctx context.Context, filter ports.JobFilter) ([]*job.Job, int64, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.UserID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("user_id = $%d", argIdx))
		args = append(args, *filter.UserID)
		argIdx++
	}
	if filter.QueueID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("queue_id = $%d", argIdx))
		args = append(args, *filter.QueueID)
		argIdx++
	}
	if filter.WorkerID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("worker_id = $%d", argIdx))
		args = append(args, *filter.WorkerID)
		argIdx++
	}
	if filter.Status != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(*filter.Status))
		argIdx++
	}
	if filter.TaskType != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("task_type = $%d", argIdx))
		args = append(args, *filter.TaskType)
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Count query
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", whereSQL)
	var total int64
	err := r.client.Pool.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count jobs: %w", err)
	}

	// Pagination & Ordering
	sortBy := "created_at"
	if filter.SortBy == "priority" || filter.SortBy == "scheduled_at" {
		sortBy = filter.SortBy
	}
	sortOrder := "DESC"
	if strings.ToUpper(filter.SortOrder) == "ASC" {
		sortOrder = "ASC"
	}

	limit := 50
	if filter.Limit > 0 && filter.Limit <= 100 {
		limit = filter.Limit
	}
	offset := 0
	if filter.Offset > 0 {
		offset = filter.Offset
	}

	listQuery := fmt.Sprintf(`
		SELECT
			id, user_id, workflow_id, workflow_node_id, queue_id, worker_id,
			task_type, payload, priority, status, attempt_count, max_attempts,
			scheduled_at, timeout_seconds, lease_expires_at, created_at, updated_at,
			started_at, completed_at, cancelled_at, error_code, error_message, idempotency_key
		FROM jobs
		%s
		ORDER BY %s %s
		LIMIT $%d OFFSET $%d
	`, whereSQL, sortBy, sortOrder, argIdx, argIdx+1)

	args = append(args, limit, offset)
	rows, err := r.client.Pool.Query(ctx, listQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		j, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		jobs = append(jobs, j)
	}

	return jobs, total, nil
}

// ClaimNext safely locks and claims the highest priority runnable job using FOR UPDATE SKIP LOCKED.
func (r *JobRepo) ClaimNext(ctx context.Context, queueID, workerID uuid.UUID, leaseDuration time.Duration) (*job.Job, *job.JobAttempt, error) {
	var claimedJob *job.Job
	var attempt *job.JobAttempt

	err := r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		selectQuery := `
			SELECT
				id, user_id, workflow_id, workflow_node_id, queue_id, worker_id,
				task_type, payload, priority, status, attempt_count, max_attempts,
				scheduled_at, timeout_seconds, lease_expires_at, created_at, updated_at,
				started_at, completed_at, cancelled_at, error_code, error_message, idempotency_key
			FROM jobs
			WHERE queue_id = $1
			  AND status IN ('QUEUED', 'RETRYING', 'PENDING')
			  AND scheduled_at <= NOW()
			ORDER BY priority DESC, scheduled_at ASC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		`
		row := tx.QueryRow(ctx, selectQuery, queueID)
		j, err := scanJob(row)
		if err != nil {
			if errors.Is(err, job.ErrJobNotFound) {
				return job.ErrJobNotFound
			}
			return fmt.Errorf("failed to select job for claim: %w", err)
		}

		now := time.Now().UTC()
		att, err := j.Claim(workerID, leaseDuration, now)
		if err != nil {
			return err
		}

		updateQuery := `
			UPDATE jobs
			SET
				status = $1,
				worker_id = $2,
				attempt_count = $3,
				lease_expires_at = $4,
				started_at = COALESCE(started_at, $5),
				updated_at = $6
			WHERE id = $7
		`
		_, err = tx.Exec(ctx, updateQuery,
			string(j.Status), j.WorkerID, j.AttemptCount, j.LeaseExpiresAt, j.StartedAt, j.UpdatedAt, j.ID,
		)
		if err != nil {
			return fmt.Errorf("failed to update claimed job: %w", err)
		}

		attemptQuery := `
			INSERT INTO job_attempts (
				id, job_id, attempt_number, worker_id,
				status, started_at, lease_expires_at, metadata
			) VALUES (
				$1, $2, $3, $4,
				$5, $6, $7, $8
			)
		`
		_, err = tx.Exec(ctx, attemptQuery,
			att.ID, att.JobID, att.AttemptNumber, att.WorkerID,
			string(att.Status), att.StartedAt, att.LeaseExpiresAt, att.Metadata,
		)
		if err != nil {
			return fmt.Errorf("failed to record job attempt: %w", err)
		}

		claimedJob = j
		attempt = att
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return claimedJob, attempt, nil
}

// RenewLease extends the lease duration for an active worker.
func (r *JobRepo) RenewLease(ctx context.Context, jobID, workerID uuid.UUID, leaseDuration time.Duration) error {
	now := time.Now().UTC()
	newExpiry := now.Add(leaseDuration)

	query := `
		UPDATE jobs
		SET
			lease_expires_at = $1,
			updated_at = $2
		WHERE id = $3
		  AND worker_id = $4
		  AND status = 'RUNNING'
		  AND lease_expires_at > $5
	`
	tag, err := r.client.Pool.Exec(ctx, query, newExpiry, now, jobID, workerID, now)
	if err != nil {
		return fmt.Errorf("failed to renew job lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrLeaseExpired
	}
	return nil
}

// Complete marks a job and its active attempt as SUCCEEDED.
func (r *JobRepo) Complete(ctx context.Context, jobID, workerID uuid.UUID) error {
	now := time.Now().UTC()

	return r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		jobQuery := `
			UPDATE jobs
			SET
				status = 'SUCCEEDED',
				completed_at = $1,
				lease_expires_at = NULL,
				updated_at = $1,
				error_code = NULL,
				error_message = NULL
			WHERE id = $2
			  AND worker_id = $3
			  AND status = 'RUNNING'
		`
		tag, err := tx.Exec(ctx, jobQuery, now, jobID, workerID)
		if err != nil {
			return fmt.Errorf("failed to complete job: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return job.ErrInvalidStateTransition
		}

		attemptQuery := `
			UPDATE job_attempts
			SET
				status = 'SUCCEEDED',
				finished_at = $1
			WHERE job_id = $2
			  AND worker_id = $3
			  AND status = 'RUNNING'
		`
		_, _ = tx.Exec(ctx, attemptQuery, now, jobID, workerID)
		return nil
	})
}

// Fail updates a job status to RETRYING or DEAD based on retry configuration.
func (r *JobRepo) Fail(ctx context.Context, jobID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error {
	now := time.Now().UTC()

	return r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		selectQuery := `
			SELECT attempt_count, max_attempts
			FROM jobs
			WHERE id = $1
			  AND worker_id = $2
			  AND status = 'RUNNING'
			FOR UPDATE
		`
		var attemptCount, maxAttempts int
		err := tx.QueryRow(ctx, selectQuery, jobID, workerID).Scan(&attemptCount, &maxAttempts)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return job.ErrInvalidStateTransition
			}
			return err
		}

		var nextStatus string
		var scheduledAt time.Time
		var completedAt *time.Time

		if retryable && attemptCount < maxAttempts {
			nextStatus = string(job.StatusRetrying)
			scheduledAt = now.Add(retryDelay)
		} else {
			nextStatus = string(job.StatusDead)
			completedAt = &now
			scheduledAt = now
		}

		updateJobQuery := `
			UPDATE jobs
			SET
				status = $1,
				scheduled_at = $2,
				completed_at = $3,
				lease_expires_at = NULL,
				worker_id = NULL,
				error_code = $4,
				error_message = $5,
				updated_at = $6
			WHERE id = $7
		`
		_, err = tx.Exec(ctx, updateJobQuery,
			nextStatus, scheduledAt, completedAt, errCode, errMsg, now, jobID,
		)
		if err != nil {
			return fmt.Errorf("failed to fail job: %w", err)
		}

		updateAttemptQuery := `
			UPDATE job_attempts
			SET
				status = 'FAILED',
				finished_at = $1,
				error_code = $2,
				error_message = $3
			WHERE job_id = $4
			  AND worker_id = $5
			  AND status = 'RUNNING'
		`
		_, _ = tx.Exec(ctx, updateAttemptQuery, now, errCode, errMsg, jobID, workerID)
		return nil
	})
}

// Cancel transitions a non-terminal job to CANCELLED.
func (r *JobRepo) Cancel(ctx context.Context, jobID uuid.UUID) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET
			status = 'CANCELLED',
			cancelled_at = $1,
			lease_expires_at = NULL,
			updated_at = $1
		WHERE id = $2
		  AND status IN ('PENDING', 'QUEUED', 'RETRYING', 'RUNNING')
	`
	tag, err := r.client.Pool.Exec(ctx, query, now, jobID)
	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrInvalidStateTransition
	}
	return nil
}

// RecoverExpiredLeases identifies jobs whose leases have expired and marks them RETRYING or DEAD.
func (r *JobRepo) RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*job.Job, error) {
	var recoveredJobs []*job.Job

	err := r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		selectQuery := `
			SELECT
				id, user_id, workflow_id, workflow_node_id, queue_id, worker_id,
				task_type, payload, priority, status, attempt_count, max_attempts,
				scheduled_at, timeout_seconds, lease_expires_at, created_at, updated_at,
				started_at, completed_at, cancelled_at, error_code, error_message, idempotency_key
			FROM jobs
			WHERE status = 'RUNNING'
			  AND lease_expires_at < NOW()
			ORDER BY lease_expires_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		`
		rows, err := tx.Query(ctx, selectQuery, limit)
		if err != nil {
			return fmt.Errorf("failed to select expired leases: %w", err)
		}
		defer rows.Close()

		var expiredJobs []*job.Job
		for rows.Next() {
			j, scanErr := scanJob(rows)
			if scanErr != nil {
				return scanErr
			}
			expiredJobs = append(expiredJobs, j)
		}
		rows.Close()

		now := time.Now().UTC()
		for _, j := range expiredJobs {
			if err := j.MarkAbandoned(retryDelay, now); err != nil {
				continue
			}

			updateQuery := `
				UPDATE jobs
				SET
					status = $1,
					scheduled_at = $2,
					completed_at = $3,
					lease_expires_at = NULL,
					worker_id = NULL,
					error_code = $4,
					error_message = $5,
					updated_at = $6
				WHERE id = $7
			`
			_, err = tx.Exec(ctx, updateQuery,
				string(j.Status), j.ScheduledAt, j.CompletedAt, j.ErrorCode, j.ErrorMessage, j.UpdatedAt, j.ID,
			)
			if err != nil {
				return fmt.Errorf("failed to update abandoned job %s: %w", j.ID, err)
			}

			// Mark running attempts abandoned
			attemptQuery := `
				UPDATE job_attempts
				SET
					status = 'ABANDONED',
					finished_at = $1,
					error_code = 'LEASE_TIMEOUT',
					error_message = 'Worker lease expired without heartbeat'
				WHERE job_id = $2
				  AND status = 'RUNNING'
			`
			_, _ = tx.Exec(ctx, attemptQuery, now, j.ID)

			recoveredJobs = append(recoveredJobs, j)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return recoveredJobs, nil
}

// Create records a new attempt in job_attempts table.
func (r *JobRepo) CreateAttempt(ctx context.Context, attempt *job.JobAttempt) error {
	query := `
		INSERT INTO job_attempts (
			id, job_id, attempt_number, worker_id,
			status, started_at, finished_at, lease_expires_at,
			error_code, error_message, metadata
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8,
			$9, $10, $11
		)
	`
	_, err := r.client.Pool.Exec(ctx, query,
		attempt.ID, attempt.JobID, attempt.AttemptNumber, attempt.WorkerID,
		string(attempt.Status), attempt.StartedAt, attempt.FinishedAt, attempt.LeaseExpiresAt,
		attempt.ErrorCode, attempt.ErrorMessage, attempt.Metadata,
	)
	if err != nil {
		return fmt.Errorf("failed to insert job attempt: %w", err)
	}
	return nil
}

// Update updates an attempt status and completion fields.
func (r *JobRepo) UpdateAttempt(ctx context.Context, attempt *job.JobAttempt) error {
	query := `
		UPDATE job_attempts
		SET
			status = $1,
			finished_at = $2,
			error_code = $3,
			error_message = $4,
			metadata = $5
		WHERE id = $6
	`
	_, err := r.client.Pool.Exec(ctx, query,
		string(attempt.Status), attempt.FinishedAt, attempt.ErrorCode,
		attempt.ErrorMessage, attempt.Metadata, attempt.ID,
	)
	if err != nil {
		return fmt.Errorf("failed to update job attempt: %w", err)
	}
	return nil
}

// ListByJobID returns all historical attempts for a job.
func (r *JobRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*job.JobAttempt, error) {
	query := `
		SELECT
			id, job_id, attempt_number, worker_id, status,
			started_at, finished_at, lease_expires_at, error_code, error_message, metadata
		FROM job_attempts
		WHERE job_id = $1
		ORDER BY attempt_number ASC
	`
	rows, err := r.client.Pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to list job attempts: %w", err)
	}
	defer rows.Close()

	var attempts []*job.JobAttempt
	for rows.Next() {
		att := &job.JobAttempt{}
		var statusStr string
		err := rows.Scan(
			&att.ID, &att.JobID, &att.AttemptNumber, &att.WorkerID, &statusStr,
			&att.StartedAt, &att.FinishedAt, &att.LeaseExpiresAt, &att.ErrorCode,
			&att.ErrorMessage, &att.Metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job attempt: %w", err)
		}
		att.Status = job.AttemptStatus(statusStr)
		attempts = append(attempts, att)
	}

	return attempts, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(scanner rowScanner) (*job.Job, error) {
	j := &job.Job{}
	var statusStr string
	err := scanner.Scan(
		&j.ID, &j.UserID, &j.WorkflowID, &j.WorkflowNodeID, &j.QueueID, &j.WorkerID,
		&j.TaskType, &j.Payload, &j.Priority, &statusStr, &j.AttemptCount, &j.MaxAttempts,
		&j.ScheduledAt, &j.TimeoutSeconds, &j.LeaseExpiresAt, &j.CreatedAt, &j.UpdatedAt,
		&j.StartedAt, &j.CompletedAt, &j.CancelledAt, &j.ErrorCode, &j.ErrorMessage, &j.IdempotencyKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, job.ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to scan job row: %w", err)
	}
	j.Status = job.Status(statusStr)
	return j, nil
}
