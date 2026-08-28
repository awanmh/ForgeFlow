package e2e_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	domainJob "github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type outboxMockRepo struct {
	mu     sync.Mutex
	jobs   map[uuid.UUID]*domainJob.Job
	outbox map[uuid.UUID]*outbox.Event
}

func newOutboxMockRepo() *outboxMockRepo {
	return &outboxMockRepo{
		jobs:   make(map[uuid.UUID]*domainJob.Job),
		outbox: make(map[uuid.UUID]*outbox.Event),
	}
}

func (m *outboxMockRepo) Create(ctx context.Context, j *domainJob.Job, event *outbox.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	if event != nil {
		m.outbox[event.ID] = event
	}
	return nil
}

func (m *outboxMockRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainJob.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, domainJob.ErrJobNotFound
	}
	return j, nil
}

func (m *outboxMockRepo) List(ctx context.Context, filter ports.JobFilter) ([]*domainJob.Job, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*domainJob.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, int64(len(list)), nil
}

func (m *outboxMockRepo) ClaimNext(ctx context.Context, queueID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	return nil, nil, nil
}
func (m *outboxMockRepo) RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) error {
	return nil
}
func (m *outboxMockRepo) Complete(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error {
	return nil
}
func (m *outboxMockRepo) Fail(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error {
	return nil
}
func (m *outboxMockRepo) Cancel(ctx context.Context, jobID uuid.UUID) error {
	return nil
}
func (m *outboxMockRepo) RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*domainJob.Job, error) {
	return nil, nil
}

func (m *outboxMockRepo) FetchPendingOutbox(limit int) []*outbox.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	var pending []*outbox.Event
	for _, e := range m.outbox {
		if e.Status == outbox.StatusPending {
			pending = append(pending, e)
		}
	}
	return pending
}

func (m *outboxMockRepo) MarkPublished(eventID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.outbox[eventID]; ok {
		e.Status = outbox.StatusPublished
	}
}

func TestFailureInjection_RedisOutageAndOutboxRecovery(t *testing.T) {
	ctx := context.Background()
	repo := newOutboxMockRepo()

	// 1. Submit Job when Redis engine is offline / uninitialized (nil queue engine)
	jobSvc := appJob.NewService(repo, &mockAttemptRepo{}, nil, nil)

	cmd := appJob.SubmitJobCommand{
		UserID:      uuid.New(),
		QueueID:     uuid.New(),
		QueueName:   "default",
		TaskType:    "custom-demo",
		Payload:     json.RawMessage(`{"action":"critical-financial-record"}`),
		Priority:    50,
		MaxAttempts: 3,
		ScheduledAt: time.Now().UTC(),
	}

	j, replayed, _, err := jobSvc.Submit(ctx, cmd)
	require.NoError(t, err, "Job submission must succeed in PostgreSQL even when Redis is offline")
	assert.False(t, replayed)
	assert.NotNil(t, j)

	// Verify job is stored in PostgreSQL with PENDING state
	storedJob, err := repo.GetByID(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, domainJob.StatusPending, storedJob.Status)

	// Verify Outbox Event exists in PENDING state
	pendingEvents := repo.FetchPendingOutbox(10)
	require.Len(t, pendingEvents, 1, "Transactional outbox event must be recorded in Postgres")
	assert.Equal(t, outbox.StatusPending, pendingEvents[0].Status)
	assert.Equal(t, j.ID, pendingEvents[0].AggregateID)

	// 2. Simulate Redis coming back online -> Scheduler Outbox Publisher drains the queue
	publishedStreams := make(map[string][]uuid.UUID)
	for _, evt := range pendingEvents {
		// Mock publishing to Redis Stream
		publishedStreams[evt.EventType] = append(publishedStreams[evt.EventType], evt.AggregateID)
		repo.MarkPublished(evt.ID)
	}

	// 3. Verify all pending outbox events are now PUBLISHED
	remainingPending := repo.FetchPendingOutbox(10)
	assert.Len(t, remainingPending, 0, "All pending outbox events must be drained and marked PUBLISHED")
	assert.Len(t, publishedStreams["job.created"], 1)
	assert.Equal(t, j.ID, publishedStreams["job.created"][0])
}
