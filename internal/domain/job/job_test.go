package job_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/domain/job"
)

func TestNewJob_Validation(t *testing.T) {
	userID := uuid.New()
	queueID := uuid.New()

	t.Run("valid job creation", func(t *testing.T) {
		j, err := job.NewJob(userID, queueID, "http-request", []byte(`{"url":"https://example.com"}`), 5, 3, time.Now(), nil, nil)
		require.NoError(t, err)
		assert.Equal(t, job.StatusPending, j.Status)
		assert.Equal(t, 0, j.AttemptCount)
		assert.Equal(t, 3, j.MaxAttempts)
		assert.Equal(t, 5, j.Priority)
		assert.False(t, j.IsTerminal())
	})

	t.Run("missing task type", func(t *testing.T) {
		_, err := job.NewJob(userID, queueID, "", []byte(`{}`), 0, 3, time.Now(), nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, job.ErrInvalidPayload)
	})

	t.Run("negative priority", func(t *testing.T) {
		_, err := job.NewJob(userID, queueID, "test", []byte(`{}`), -1, 3, time.Now(), nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, job.ErrInvalidPriority)
	})

	t.Run("invalid json payload", func(t *testing.T) {
		_, err := job.NewJob(userID, queueID, "test", []byte(`{invalid-json`), 0, 3, time.Now(), nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, job.ErrInvalidPayload)
	})
}

func TestJob_StateTransitions(t *testing.T) {
	userID := uuid.New()
	queueID := uuid.New()
	workerID := uuid.New()
	otherWorkerID := uuid.New()
	now := time.Now().UTC()

	j, err := job.NewJob(userID, queueID, "docker-build", []byte(`{}`), 10, 2, now, nil, nil)
	require.NoError(t, err)

	// PENDING -> QUEUED
	err = j.Enqueue(now)
	require.NoError(t, err)
	assert.Equal(t, job.StatusQueued, j.Status)

	// Cannot enqueue already QUEUED
	err = j.Enqueue(now)
	require.ErrorIs(t, err, job.ErrInvalidStateTransition)

	// QUEUED -> RUNNING (Claim)
	attempt, err := j.Claim(workerID, 30*time.Second, now)
	require.NoError(t, err)
	assert.Equal(t, job.StatusRunning, j.Status)
	assert.Equal(t, 1, j.AttemptCount)
	assert.Equal(t, 1, attempt.AttemptNumber)
	assert.Equal(t, job.AttemptRunning, attempt.Status)
	assert.NotNil(t, j.LeaseExpiresAt)

	// Cannot claim RUNNING job
	_, err = j.Claim(otherWorkerID, 30*time.Second, now)
	require.ErrorIs(t, err, job.ErrInvalidStateTransition)

	// Renew Lease (correct worker)
	err = j.RenewLease(workerID, 30*time.Second, now.Add(5*time.Second))
	require.NoError(t, err)

	// Renew Lease (wrong worker)
	err = j.RenewLease(otherWorkerID, 30*time.Second, now.Add(5*time.Second))
	require.ErrorIs(t, err, job.ErrAlreadyClaimed)

	// Fail Attempt 1 (Retryable) -> RETRYING
	err = j.Fail(workerID, "NETWORK_ERR", "Timeout connecting to host", true, 5*time.Second, now.Add(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, job.StatusRetrying, j.Status)
	assert.Nil(t, j.WorkerID)
	assert.Nil(t, j.LeaseExpiresAt)

	// RETRYING -> QUEUED
	err = j.Enqueue(now.Add(15*time.Second))
	require.NoError(t, err)
	assert.Equal(t, job.StatusQueued, j.Status)

	// QUEUED -> RUNNING (Attempt 2)
	attempt2, err := j.Claim(otherWorkerID, 30*time.Second, now.Add(15*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 2, j.AttemptCount)
	assert.Equal(t, 2, attempt2.AttemptNumber)

	// Fail Attempt 2 (Max attempts reached) -> DEAD
	err = j.Fail(otherWorkerID, "FATAL_ERR", "Container crash", true, 5*time.Second, now.Add(20*time.Second))
	require.NoError(t, err)
	assert.Equal(t, job.StatusDead, j.Status)
	assert.True(t, j.IsTerminal())

	// Terminal states cannot transition
	err = j.Enqueue(now)
	require.ErrorIs(t, err, job.ErrInvalidStateTransition)

	_, err = j.Claim(workerID, 30*time.Second, now)
	require.ErrorIs(t, err, job.ErrInvalidStateTransition)

	err = j.Cancel(now)
	require.ErrorIs(t, err, job.ErrInvalidStateTransition)
}

func TestJob_Cancellation(t *testing.T) {
	userID := uuid.New()
	queueID := uuid.New()
	now := time.Now().UTC()

	t.Run("cancel pending job", func(t *testing.T) {
		j, _ := job.NewJob(userID, queueID, "test", []byte(`{}`), 0, 3, now, nil, nil)
		err := j.Cancel(now)
		require.NoError(t, err)
		assert.Equal(t, job.StatusCancelled, j.Status)
		assert.True(t, j.IsTerminal())
	})

	t.Run("cancel running job", func(t *testing.T) {
		j, _ := job.NewJob(userID, queueID, "test", []byte(`{}`), 0, 3, now, nil, nil)
		_ = j.Enqueue(now)
		workerID := uuid.New()
		_, _ = j.Claim(workerID, 30*time.Second, now)

		err := j.Cancel(now)
		require.NoError(t, err)
		assert.Equal(t, job.StatusCancelled, j.Status)
		assert.Nil(t, j.LeaseExpiresAt)
	})
}

func TestJob_AbandonedRecovery(t *testing.T) {
	userID := uuid.New()
	queueID := uuid.New()
	workerID := uuid.New()
	now := time.Now().UTC()

	j, _ := job.NewJob(userID, queueID, "test", []byte(`{}`), 0, 2, now, nil, nil)
	_ = j.Enqueue(now)
	_, _ = j.Claim(workerID, 30*time.Second, now)

	// Abandon on Attempt 1 -> RETRYING
	err := j.MarkAbandoned(5*time.Second, now.Add(35*time.Second))
	require.NoError(t, err)
	assert.Equal(t, job.StatusRetrying, j.Status)
	assert.Nil(t, j.WorkerID)

	// Claim Attempt 2
	_ = j.Enqueue(now.Add(40*time.Second))
	_, _ = j.Claim(workerID, 30*time.Second, now.Add(40*time.Second))

	// Abandon on Attempt 2 (Max attempts reached) -> DEAD
	err = j.MarkAbandoned(5*time.Second, now.Add(75*time.Second))
	require.NoError(t, err)
	assert.Equal(t, job.StatusDead, j.Status)
	assert.True(t, j.IsTerminal())
}
