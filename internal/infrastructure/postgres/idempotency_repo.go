package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/forgeflow/forgeflow/internal/ports"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflict: request payload mismatch")
)

type IdempotencyRepo struct {
	client *Client
}

func NewIdempotencyRepo(client *Client) *IdempotencyRepo {
	return &IdempotencyRepo{client: client}
}

// GetOrLock atomically checks for an existing idempotency record or claims it for the current transaction.
// Returns (record, isNew, error).
func (r *IdempotencyRepo) GetOrLock(ctx context.Context, userID uuid.UUID, key, requestHash string, ttl time.Duration) (*ports.IdempotencyRecord, bool, error) {
	now := time.Now().UTC()
	expiry := now.Add(ttl)

	insertQuery := `
		INSERT INTO idempotency_keys (
			id, user_id, key, request_hash, expires_at, created_at
		) VALUES (
			gen_random_uuid(), $1, $2, $3, $4, $5
		)
		ON CONFLICT (user_id, key) DO NOTHING
		RETURNING id, user_id, key, request_hash, resource_id, resource_type, response_status, response_body, expires_at, created_at
	`

	rec := &ports.IdempotencyRecord{}
	err := r.client.Pool.QueryRow(ctx, insertQuery, userID, key, requestHash, expiry, now).Scan(
		&rec.ID, &rec.UserID, &rec.Key, &rec.RequestHash, &rec.ResourceID, &rec.ResourceType,
		&rec.ResponseStatus, &rec.ResponseBody, &rec.ExpiresAt, &rec.CreatedAt,
	)

	if err == nil {
		// New idempotency record created successfully
		return rec, true, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("failed to insert idempotency key: %w", err)
	}

	// Record already exists: fetch existing
	selectQuery := `
		SELECT id, user_id, key, request_hash, resource_id, resource_type, response_status, response_body, expires_at, created_at
		FROM idempotency_keys
		WHERE user_id = $1 AND key = $2
	`
	err = r.client.Pool.QueryRow(ctx, selectQuery, userID, key).Scan(
		&rec.ID, &rec.UserID, &rec.Key, &rec.RequestHash, &rec.ResourceID, &rec.ResourceType,
		&rec.ResponseStatus, &rec.ResponseBody, &rec.ExpiresAt, &rec.CreatedAt,
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to select existing idempotency record: %w", err)
	}

	// If request hash does not match, return conflict error
	if rec.RequestHash != requestHash {
		return nil, false, ErrIdempotencyConflict
	}

	return rec, false, nil
}

// SaveResponse stores the HTTP status, body, and resource ID for an idempotency record.
func (r *IdempotencyRepo) SaveResponse(ctx context.Context, userID uuid.UUID, key string, statusCode int, responseBody []byte, resourceID *uuid.UUID, resourceType *string) error {
	query := `
		UPDATE idempotency_keys
		SET response_status = $1, response_body = $2, resource_id = $3, resource_type = $4
		WHERE user_id = $5 AND key = $6
	`
	_, err := r.client.Pool.Exec(ctx, query,
		statusCode, responseBody, resourceID, resourceType, userID, key,
	)
	if err != nil {
		return fmt.Errorf("failed to save idempotency response: %w", err)
	}
	return nil
}
