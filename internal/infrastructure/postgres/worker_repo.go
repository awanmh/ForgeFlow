package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/forgeflow/forgeflow/internal/domain/worker"
)

type WorkerRepo struct {
	client *Client
}

func NewWorkerRepo(client *Client) *WorkerRepo {
	return &WorkerRepo{client: client}
}

func (r *WorkerRepo) Register(ctx context.Context, w *worker.Worker) error {
	capabilities := w.Capabilities
	if len(capabilities) == 0 {
		capabilities = []byte("[]")
	}
	metadata := w.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	query := `
		INSERT INTO workers (
			id, worker_key, hostname, version, status, concurrency,
			registered_at, last_heartbeat_at, started_at, capabilities, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11
		)
		ON CONFLICT (worker_key) DO UPDATE SET
			id = EXCLUDED.id,
			hostname = EXCLUDED.hostname,
			version = EXCLUDED.version,
			status = EXCLUDED.status,
			concurrency = EXCLUDED.concurrency,
			last_heartbeat_at = EXCLUDED.last_heartbeat_at,
			started_at = EXCLUDED.started_at,
			capabilities = EXCLUDED.capabilities,
			metadata = EXCLUDED.metadata
	`
	_, err := r.client.Pool.Exec(ctx, query,
		w.ID, w.WorkerKey, w.Hostname, w.Version, string(w.Status), w.Concurrency,
		w.RegisteredAt, w.LastHeartbeatAt, w.StartedAt, capabilities, metadata,
	)
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}
	return nil
}

func (r *WorkerRepo) Heartbeat(ctx context.Context, id uuid.UUID, now time.Time) error {
	query := `
		UPDATE workers
		SET last_heartbeat_at = $1
		WHERE id = $2 AND status IN ('STARTING', 'ACTIVE', 'DRAINING')
	`
	tag, err := r.client.Pool.Exec(ctx, query, now.UTC(), id)
	if err != nil {
		return fmt.Errorf("failed to record worker heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return worker.ErrWorkerNotFound
	}
	return nil
}

func (r *WorkerRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status worker.Status) error {
	var stoppedAt *time.Time
	if status == worker.StatusStopped || status == worker.StatusDead {
		now := time.Now().UTC()
		stoppedAt = &now
	}

	query := `
		UPDATE workers
		SET status = $1, stopped_at = COALESCE($2, stopped_at)
		WHERE id = $3
	`
	_, err := r.client.Pool.Exec(ctx, query, string(status), stoppedAt, id)
	if err != nil {
		return fmt.Errorf("failed to update worker status: %w", err)
	}
	return nil
}

func (r *WorkerRepo) GetByID(ctx context.Context, id uuid.UUID) (*worker.Worker, error) {
	query := `
		SELECT
			id, worker_key, hostname, version, status, concurrency,
			registered_at, last_heartbeat_at, started_at, stopped_at, capabilities, metadata
		FROM workers
		WHERE id = $1
	`
	w := &worker.Worker{}
	var statusStr string
	err := r.client.Pool.QueryRow(ctx, query, id).Scan(
		&w.ID, &w.WorkerKey, &w.Hostname, &w.Version, &statusStr, &w.Concurrency,
		&w.RegisteredAt, &w.LastHeartbeatAt, &w.StartedAt, &w.StoppedAt, &w.Capabilities, &w.Metadata,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, worker.ErrWorkerNotFound
		}
		return nil, fmt.Errorf("failed to fetch worker: %w", err)
	}
	w.Status = worker.Status(statusStr)
	return w, nil
}

func (r *WorkerRepo) GetByKey(ctx context.Context, key string) (*worker.Worker, error) {
	query := `
		SELECT
			id, worker_key, hostname, version, status, concurrency,
			registered_at, last_heartbeat_at, started_at, stopped_at, capabilities, metadata
		FROM workers
		WHERE worker_key = $1
	`
	w := &worker.Worker{}
	var statusStr string
	err := r.client.Pool.QueryRow(ctx, query, key).Scan(
		&w.ID, &w.WorkerKey, &w.Hostname, &w.Version, &statusStr, &w.Concurrency,
		&w.RegisteredAt, &w.LastHeartbeatAt, &w.StartedAt, &w.StoppedAt, &w.Capabilities, &w.Metadata,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, worker.ErrWorkerNotFound
		}
		return nil, fmt.Errorf("failed to fetch worker by key: %w", err)
	}
	w.Status = worker.Status(statusStr)
	return w, nil
}

func (r *WorkerRepo) List(ctx context.Context) ([]*worker.Worker, error) {
	query := `
		SELECT
			id, worker_key, hostname, version, status, concurrency,
			registered_at, last_heartbeat_at, started_at, stopped_at, capabilities, metadata
		FROM workers
		ORDER BY registered_at DESC
	`
	rows, err := r.client.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list workers: %w", err)
	}
	defer rows.Close()

	var workers []*worker.Worker
	for rows.Next() {
		w := &worker.Worker{}
		var statusStr string
		err := rows.Scan(
			&w.ID, &w.WorkerKey, &w.Hostname, &w.Version, &statusStr, &w.Concurrency,
			&w.RegisteredAt, &w.LastHeartbeatAt, &w.StartedAt, &w.StoppedAt, &w.Capabilities, &w.Metadata,
		)
		if err != nil {
			return nil, err
		}
		w.Status = worker.Status(statusStr)
		workers = append(workers, w)
	}
	return workers, nil
}

func (r *WorkerRepo) FindDeadWorkers(ctx context.Context, threshold time.Duration) ([]*worker.Worker, error) {
	cutoff := time.Now().UTC().Add(-threshold)
	query := `
		SELECT
			id, worker_key, hostname, version, status, concurrency,
			registered_at, last_heartbeat_at, started_at, stopped_at, capabilities, metadata
		FROM workers
		WHERE status = 'ACTIVE'
		  AND last_heartbeat_at < $1
	`
	rows, err := r.client.Pool.Query(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to find dead workers: %w", err)
	}
	defer rows.Close()

	var deadWorkers []*worker.Worker
	for rows.Next() {
		w := &worker.Worker{}
		var statusStr string
		err := rows.Scan(
			&w.ID, &w.WorkerKey, &w.Hostname, &w.Version, &statusStr, &w.Concurrency,
			&w.RegisteredAt, &w.LastHeartbeatAt, &w.StartedAt, &w.StoppedAt, &w.Capabilities, &w.Metadata,
		)
		if err != nil {
			return nil, err
		}
		w.Status = worker.Status(statusStr)
		deadWorkers = append(deadWorkers, w)
	}
	return deadWorkers, nil
}
