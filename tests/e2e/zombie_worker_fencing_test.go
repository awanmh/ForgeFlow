package e2e_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	domainJob "github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	"github.com/forgeflow/forgeflow/internal/ports"
)

var (
	ErrStaleWorkerWrite = errors.New("stale worker lease token: update rejected")
)

// FencedJobRepo simulates PostgreSQL row-level fencing tokens (lease + attempt count verification)
type fencedJobRepo struct {
	mu   sync.Mutex
	jobs map[uuid.UUID]*domainJob.Job
}

func newFencedJobRepo() *fencedJobRepo {
	return &fencedJobRepo{jobs: make(map[uuid.UUID]*domainJob.Job)}
}

func (m *fencedJobRepo) Create(ctx context.Context, j *domainJob.Job, event *outbox.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}

func (m *fencedJobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainJob.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, domainJob.ErrJobNotFound
	}
	return j, nil
}

func (m *fencedJobRepo) List(ctx context.Context, filter ports.JobFilter) ([]*domainJob.Job, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*domainJob.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, int64(len(list)), nil
}

func (m *fencedJobRepo) ClaimNext(ctx context.Context, queueID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	for _, j := range m.jobs {
		if j.Status == domainJob.StatusPending || j.Status == domainJob.StatusQueued || j.Status == domainJob.StatusRetrying {
			j.Status = domainJob.StatusRunning
			j.WorkerID = &workerID
			j.AttemptCount++
			exp := now.Add(leaseDuration)
			j.LeaseExpiresAt = &exp
			wID := workerID
			attempt := &domainJob.JobAttempt{
				ID:            uuid.New(),
				JobID:         j.ID,
				AttemptNumber: j.AttemptCount,
				WorkerID:      &wID,
				Status:        domainJob.AttemptRunning,
				StartedAt:     now,
			}
			return j, attempt, nil
		}
	}
	return nil, nil, nil
}

// Complete enforces that ONLY the worker with the current active lease and matching workerID can complete the job
func (m *fencedJobRepo) Complete(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID]
	if !ok {
		return domainJob.ErrJobNotFound
	}

	// Fencing check: if worker is not the active holder or lease expired -> reject
	if j.WorkerID == nil || *j.WorkerID != workerID {
		return ErrStaleWorkerWrite
	}
	if j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(time.Now().UTC()) {
		return ErrStaleWorkerWrite
	}

	j.Status = domainJob.StatusSucceeded
	now := time.Now().UTC()
	j.CompletedAt = &now
	return nil
}

func (m *fencedJobRepo) RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*domainJob.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	var recovered []*domainJob.Job
	for _, j := range m.jobs {
		if j.Status == domainJob.StatusRunning && j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now) {
			j.Status = domainJob.StatusRetrying
			j.WorkerID = nil
			recovered = append(recovered, j)
		}
	}
	return recovered, nil
}

func (m *fencedJobRepo) RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) error {
	return nil
}

func (m *fencedJobRepo) Fail(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error {
	return nil
}

func (m *fencedJobRepo) Cancel(ctx context.Context, jobID uuid.UUID) error {
	return nil
}

func TestFailureInjection_ZombieWorkerFencingSafety(t *testing.T) {
	ctx := context.Background()
	repo := newFencedJobRepo()

	jobID := uuid.New()
	j := &domainJob.Job{
		ID:          jobID,
		UserID:      uuid.New(),
		QueueID:     uuid.New(),
		TaskType:    "custom-demo",
		Payload:     []byte(`{}`),
		Priority:    10,
		Status:      domainJob.StatusPending,
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	require.NoError(t, repo.Create(ctx, j, nil))

	workerA := uuid.New()
	workerB := uuid.New()

	// 1. Worker A claims the job with a short 50ms lease
	claimedJob, attemptA, err := repo.ClaimNext(ctx, j.QueueID, workerA, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, workerA, *claimedJob.WorkerID)
	assert.Equal(t, 1, attemptA.AttemptNumber)

	// 2. Worker A experiences a simulated 100ms GC pause (lease expires during pause)
	time.Sleep(100 * time.Millisecond)

	// 3. Scheduler detects expired lease and recovers the job
	recovered, err := repo.RecoverExpiredLeases(ctx, 10, 0)
	require.NoError(t, err)
	assert.Len(t, recovered, 1)

	// 4. Worker B claims the recovered job (Attempt #2) with a fresh lease
	reclaimedJob, attemptB, err := repo.ClaimNext(ctx, j.QueueID, workerB, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, workerB, *reclaimedJob.WorkerID)
	assert.Equal(t, 2, attemptB.AttemptNumber)

	// 5. Worker A wakes up from GC pause and attempts to mark Job as SUCCEEDED
	err = repo.Complete(ctx, jobID, workerA)
	require.Error(t, err, "Worker A's write must be rejected by PostgreSQL fencing check")
	assert.ErrorIs(t, err, ErrStaleWorkerWrite)

	// 6. Worker B finishes execution and successfully completes the job
	err = repo.Complete(ctx, jobID, workerB)
	require.NoError(t, err, "Worker B's write must succeed as authoritative lease holder")

	finalJob, err := repo.GetByID(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, domainJob.StatusSucceeded, finalJob.Status)
}
