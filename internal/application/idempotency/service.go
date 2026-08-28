package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/infrastructure/postgres"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// HashPayload calculates a deterministic SHA-256 hexadecimal checksum of request payload bytes.
func HashPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// Service manages atomic idempotency locking, validation, and cached responses.
type Service struct {
	repo ports.IdempotencyRepository
}

// NewService constructs an Idempotency Service.
func NewService(repo ports.IdempotencyRepository) *Service {
	return &Service{repo: repo}
}

// CheckOrLock attempts to acquire an idempotency key or retrieve previously cached responses.
// Returns:
// - (*ports.IdempotencyRecord, isNew = true, nil) if this is the first unique request
// - (*ports.IdempotencyRecord, isNew = false, nil) if this is an identical duplicate request (caller should return cached response)
// - (nil, false, ErrIdempotencyConflict) if payload does not match the original key submission
func (s *Service) CheckOrLock(ctx context.Context, userID uuid.UUID, key string, payload []byte, ttl time.Duration) (*ports.IdempotencyRecord, bool, error) {
	if key == "" {
		return nil, true, nil // No idempotency key requested
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	requestHash := HashPayload(payload)
	rec, isNew, err := s.repo.GetOrLock(ctx, userID, key, requestHash, ttl)
	if err != nil {
		if err == postgres.ErrIdempotencyConflict {
			return nil, false, err
		}
		return nil, false, err
	}

	return rec, isNew, nil
}

// SaveResponse records the HTTP response status and body associated with the idempotency key.
func (s *Service) SaveResponse(ctx context.Context, userID uuid.UUID, key string, statusCode int, responseBody []byte, resourceID *uuid.UUID, resourceType *string) error {
	if key == "" {
		return nil
	}
	return s.repo.SaveResponse(ctx, userID, key, statusCode, responseBody, resourceID, resourceType)
}
