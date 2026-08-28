package job_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/application/job"
	domainJob "github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type mockJobRepo struct {
	jobs map[uuid.UUID]*domainJob.Job
}

func newMockJobRepo() *mockJobRepo {
	return &mockJobRepo{jobs: make(map[uuid.UUID]*domainJob.Job)}
}

func (m *mockJobRepo) Create(ctx context.Context, j *domainJob.Job, event *outbox.Event) error {
	m.jobs[j.ID] = j
	return nil
}

func (m *mockJobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainJob.Job, error) {
	j, ok := m.jobs[id]
	if !ok {
		return nil, domainJob.ErrJobNotFound
	}
	return j, nil
}

func (m *mockJobRepo) List(ctx context.Context, filter ports.JobFilter) ([]*domainJob.Job, int64, error) {
	var list []*domainJob.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, int64(len(list)), nil
}

func (m *mockJobRepo) ClaimNext(ctx context.Context, queueID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	return nil, nil, nil
}

func (m *mockJobRepo) ClaimByID(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	return nil, nil, nil
}

func (m *mockJobRepo) RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) error {
	return nil
}

func (m *mockJobRepo) Complete(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error {
	return nil
}

func (m *mockJobRepo) Fail(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error {
	return nil
}

func (m *mockJobRepo) Cancel(ctx context.Context, jobID uuid.UUID) error {
	if j, ok := m.jobs[jobID]; ok {
		j.Status = domainJob.StatusCancelled
	}
	return nil
}

func (m *mockJobRepo) RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*domainJob.Job, error) {
	return nil, nil
}

type mockAttemptRepo struct{}

func (m *mockAttemptRepo) Create(ctx context.Context, a *domainJob.JobAttempt) error { return nil }
func (m *mockAttemptRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domainJob.JobAttempt, error) {
	return nil, nil
}
func (m *mockAttemptRepo) Update(ctx context.Context, a *domainJob.JobAttempt) error { return nil }

func TestJobService_Submit(t *testing.T) {
	jobRepo := newMockJobRepo()
	attemptRepo := &mockAttemptRepo{}

	svc := job.NewService(jobRepo, attemptRepo, nil, nil)

	ctx := context.Background()
	cmd := job.SubmitJobCommand{
		UserID:      uuid.New(),
		QueueID:     uuid.New(),
		QueueName:   "default",
		TaskType:    "custom-demo",
		Payload:     json.RawMessage(`{"action":"success"}`),
		Priority:    10,
		MaxAttempts: 3,
		ScheduledAt: time.Now().UTC(),
	}

	j, replayed, _, err := svc.Submit(ctx, cmd)
	require.NoError(t, err)
	assert.False(t, replayed)
	assert.NotNil(t, j)
	assert.Equal(t, domainJob.StatusPending, j.Status)
	assert.Equal(t, "custom-demo", j.TaskType)

	// Fetch job
	fetched, attempts, err := svc.GetByID(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, j.ID, fetched.ID)
	assert.Nil(t, attempts)
}

func TestJobService_Cancel(t *testing.T) {
	jobRepo := newMockJobRepo()
	svc := job.NewService(jobRepo, &mockAttemptRepo{}, nil, nil)

	ctx := context.Background()
	cmd := job.SubmitJobCommand{
		UserID:      uuid.New(),
		QueueID:     uuid.New(),
		QueueName:   "default",
		TaskType:    "custom-demo",
		Payload:     json.RawMessage(`{"action":"success"}`),
		Priority:    10,
		MaxAttempts: 3,
		ScheduledAt: time.Now().UTC(),
	}

	j, _, _, err := svc.Submit(ctx, cmd)
	require.NoError(t, err)

	err = svc.Cancel(ctx, j.ID)
	require.NoError(t, err)

	updated, _, err := svc.GetByID(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, domainJob.StatusCancelled, updated.Status)
}
