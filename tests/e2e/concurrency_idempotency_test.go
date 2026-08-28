package e2e_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/application/idempotency"
	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	domainJob "github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	infraPostgres "github.com/forgeflow/forgeflow/internal/infrastructure/postgres"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// ThreadSafeMockJobRepo simulates PostgreSQL with atomic table locking and unique constraints
type threadSafeMockJobRepo struct {
	mu   sync.Mutex
	jobs map[uuid.UUID]*domainJob.Job
}

func newThreadSafeMockJobRepo() *threadSafeMockJobRepo {
	return &threadSafeMockJobRepo{jobs: make(map[uuid.UUID]*domainJob.Job)}
}

func (m *threadSafeMockJobRepo) Create(ctx context.Context, j *domainJob.Job, event *outbox.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}

func (m *threadSafeMockJobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainJob.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, domainJob.ErrJobNotFound
	}
	return j, nil
}

func (m *threadSafeMockJobRepo) List(ctx context.Context, filter ports.JobFilter) ([]*domainJob.Job, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*domainJob.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, int64(len(list)), nil
}

func (m *threadSafeMockJobRepo) ClaimNext(ctx context.Context, queueID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	return nil, nil, nil
}

func (m *threadSafeMockJobRepo) ClaimByID(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	return nil, nil, nil
}

func (m *threadSafeMockJobRepo) RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) error {
	return nil
}

func (m *threadSafeMockJobRepo) Complete(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error {
	return nil
}

func (m *threadSafeMockJobRepo) Fail(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error {
	return nil
}

func (m *threadSafeMockJobRepo) Cancel(ctx context.Context, jobID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobID]; ok {
		j.Status = domainJob.StatusCancelled
	}
	return nil
}

func (m *threadSafeMockJobRepo) RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*domainJob.Job, error) {
	return nil, nil
}

// ThreadSafeMockIdempotencyRepo simulates PostgreSQL UNIQUE(user_id, idempotency_key) constraint
type threadSafeMockIdempotencyRepo struct {
	mu      sync.Mutex
	records map[string]*ports.IdempotencyRecord
}

func newThreadSafeMockIdempotencyRepo() *threadSafeMockIdempotencyRepo {
	return &threadSafeMockIdempotencyRepo{records: make(map[string]*ports.IdempotencyRecord)}
}

func (m *threadSafeMockIdempotencyRepo) GetOrLock(ctx context.Context, userID uuid.UUID, key, requestHash string, ttl time.Duration) (*ports.IdempotencyRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	compositeKey := userID.String() + ":" + key
	if rec, exists := m.records[compositeKey]; exists {
		if rec.RequestHash != requestHash {
			return nil, false, infraPostgres.ErrIdempotencyConflict
		}
		// Return copy
		clone := *rec
		return &clone, false, nil
	}

	// First request wins the atomic lock (simulating INSERT ... ON CONFLICT DO NOTHING)
	now := time.Now().UTC()
	exp := now.Add(ttl)
	rec := &ports.IdempotencyRecord{
		ID:          uuid.New(),
		UserID:      userID,
		Key:         key,
		RequestHash: requestHash,
		ExpiresAt:   &exp,
		CreatedAt:   now,
	}
	m.records[compositeKey] = rec
	clone := *rec
	return &clone, true, nil
}

func (m *threadSafeMockIdempotencyRepo) SaveResponse(ctx context.Context, userID uuid.UUID, key string, statusCode int, responseBody []byte, resourceID *uuid.UUID, resourceType *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	compositeKey := userID.String() + ":" + key
	if rec, exists := m.records[compositeKey]; exists {
		rec.ResponseStatus = &statusCode
		rec.ResponseBody = responseBody
		rec.ResourceID = resourceID
		rec.ResourceType = resourceType
	}
	return nil
}

type mockAttemptRepo struct{}

func (m *mockAttemptRepo) Create(ctx context.Context, a *domainJob.JobAttempt) error { return nil }
func (m *mockAttemptRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domainJob.JobAttempt, error) {
	return nil, nil
}
func (m *mockAttemptRepo) Update(ctx context.Context, a *domainJob.JobAttempt) error { return nil }

func TestConcurrency_IdempotentJobSubmission(t *testing.T) {
	jobRepo := newThreadSafeMockJobRepo()
	idempotencyRepo := newThreadSafeMockIdempotencyRepo()
	idempotencySvc := idempotency.NewService(idempotencyRepo)
	jobSvc := appJob.NewService(jobRepo, &mockAttemptRepo{}, nil, idempotencySvc)

	const concurrencyCount = 50
	idemKey := "payment-txn-unique-key-9999"
	userID := uuid.New()
	queueID := uuid.New()

	var wg sync.WaitGroup
	var newlyCreatedCount int64
	var replayedCount int64
	var errorCount int64

	createdJobIDs := make([]uuid.UUID, concurrencyCount)

	for i := 0; i < concurrencyCount; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()

			cmd := appJob.SubmitJobCommand{
				UserID:         userID,
				QueueID:        queueID,
				QueueName:      "payments",
				TaskType:       "notification",
				Payload:        json.RawMessage(`{"amount": 100, "currency": "USD"}`),
				Priority:       10,
				MaxAttempts:    3,
				ScheduledAt:    time.Now().UTC(),
				IdempotencyKey: &idemKey,
			}

			j, replayed, _, err := jobSvc.Submit(context.Background(), cmd)
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}

			if replayed {
				atomic.AddInt64(&replayedCount, 1)
			} else {
				atomic.AddInt64(&newlyCreatedCount, 1)
				if j != nil {
					createdJobIDs[idx] = j.ID
				}
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int64(0), errorCount, "all concurrent requests should be cleanly handled")
	assert.Equal(t, int64(1), newlyCreatedCount, "exactly one logical job must be created")
	assert.Equal(t, int64(concurrencyCount-1), replayedCount, "all other 49 concurrent requests must receive idempotent replay")

	// Verify only 1 physical job exists in the repository
	jobs, total, err := jobRepo.List(context.Background(), ports.JobFilter{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, jobs, 1)
}
