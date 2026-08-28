package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/domain/outbox"
)

type OutboxRepo struct {
	client *Client
}

func NewOutboxRepo(client *Client) *OutboxRepo {
	return &OutboxRepo{client: client}
}

func (r *OutboxRepo) Create(ctx context.Context, event *outbox.Event) error {
	query := `
		INSERT INTO outbox_events (
			id, event_type, aggregate_type, aggregate_id,
			payload, status, attempts, available_at, created_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9
		)
	`
	_, err := r.client.Pool.Exec(ctx, query,
		event.ID, event.EventType, event.AggregateType, event.AggregateID,
		event.Payload, string(event.Status), event.Attempts, event.AvailableAt, event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create outbox event: %w", err)
	}
	return nil
}

func (r *OutboxRepo) FetchPending(ctx context.Context, limit int) ([]*outbox.Event, error) {
	query := `
		SELECT
			id, event_type, aggregate_type, aggregate_id,
			payload, status, attempts, available_at, created_at, published_at
		FROM outbox_events
		WHERE status = 'PENDING'
		  AND available_at <= NOW()
		ORDER BY available_at ASC, created_at ASC
		LIMIT $1
	`
	rows, err := r.client.Pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*outbox.Event
	for rows.Next() {
		e := &outbox.Event{}
		var statusStr string
		err := rows.Scan(
			&e.ID, &e.EventType, &e.AggregateType, &e.AggregateID,
			&e.Payload, &statusStr, &e.Attempts, &e.AvailableAt, &e.CreatedAt, &e.PublishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		e.Status = outbox.Status(statusStr)
		events = append(events, e)
	}
	return events, nil
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	query := `
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = $1
		WHERE id = $2
	`
	_, err := r.client.Pool.Exec(ctx, query, now, id)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event published: %w", err)
	}
	return nil
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET attempts = attempts + 1,
		    status = CASE WHEN attempts >= 5 THEN 'FAILED' ELSE 'PENDING' END,
		    available_at = NOW() + INTERVAL '5 seconds'
		WHERE id = $1
	`
	_, err := r.client.Pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to increment outbox attempt: %w", err)
	}
	return nil
}
