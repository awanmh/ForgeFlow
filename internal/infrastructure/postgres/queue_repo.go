package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/forgeflow/forgeflow/internal/domain/queue"
)

type QueueRepo struct {
	client *Client
}

func NewQueueRepo(client *Client) *QueueRepo {
	return &QueueRepo{client: client}
}

func (r *QueueRepo) Create(ctx context.Context, q *queue.Queue) error {
	query := `
		INSERT INTO queues (id, name, description, max_concurrency, priority_levels, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.client.Pool.Exec(ctx, query,
		q.ID, q.Name, q.Description, q.MaxConcurrency, q.PriorityLevels, q.Enabled, q.CreatedAt, q.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}
	return nil
}

func (r *QueueRepo) GetByID(ctx context.Context, id uuid.UUID) (*queue.Queue, error) {
	query := `
		SELECT id, name, description, max_concurrency, priority_levels, enabled, created_at, updated_at
		FROM queues
		WHERE id = $1
	`
	q := &queue.Queue{}
	err := r.client.Pool.QueryRow(ctx, query, id).Scan(
		&q.ID, &q.Name, &q.Description, &q.MaxConcurrency, &q.PriorityLevels, &q.Enabled, &q.CreatedAt, &q.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, queue.ErrQueueNotFound
		}
		return nil, fmt.Errorf("failed to fetch queue: %w", err)
	}
	return q, nil
}

func (r *QueueRepo) GetByName(ctx context.Context, name string) (*queue.Queue, error) {
	query := `
		SELECT id, name, description, max_concurrency, priority_levels, enabled, created_at, updated_at
		FROM queues
		WHERE name = $1
	`
	q := &queue.Queue{}
	err := r.client.Pool.QueryRow(ctx, query, name).Scan(
		&q.ID, &q.Name, &q.Description, &q.MaxConcurrency, &q.PriorityLevels, &q.Enabled, &q.CreatedAt, &q.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, queue.ErrQueueNotFound
		}
		return nil, fmt.Errorf("failed to fetch queue by name: %w", err)
	}
	return q, nil
}

func (r *QueueRepo) List(ctx context.Context) ([]*queue.Queue, error) {
	query := `
		SELECT id, name, description, max_concurrency, priority_levels, enabled, created_at, updated_at
		FROM queues
		ORDER BY name ASC
	`
	rows, err := r.client.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []*queue.Queue
	for rows.Next() {
		q := &queue.Queue{}
		err := rows.Scan(
			&q.ID, &q.Name, &q.Description, &q.MaxConcurrency, &q.PriorityLevels, &q.Enabled, &q.CreatedAt, &q.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		queues = append(queues, q)
	}
	return queues, nil
}

func (r *QueueRepo) Update(ctx context.Context, q *queue.Queue) error {
	query := `
		UPDATE queues
		SET description = $1, max_concurrency = $2, priority_levels = $3, enabled = $4, updated_at = $5
		WHERE id = $6
	`
	_, err := r.client.Pool.Exec(ctx, query,
		q.Description, q.MaxConcurrency, q.PriorityLevels, q.Enabled, q.UpdatedAt, q.ID,
	)
	if err != nil {
		return fmt.Errorf("failed to update queue: %w", err)
	}
	return nil
}
