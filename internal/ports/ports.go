package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	"github.com/forgeflow/forgeflow/internal/domain/queue"
	"github.com/forgeflow/forgeflow/internal/domain/user"
	"github.com/forgeflow/forgeflow/internal/domain/worker"
)

// Clock defines a deterministic time abstraction for testing.
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock using standard time.Now().
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// Locker defines distributed locking contract with token ownership.
type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}

// Lock represents an acquired distributed lock.
type Lock interface {
	Token() string
	Key() string
	Renew(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
}

// EventPublisher defines event publication port (e.g. Redis Pub/Sub).
type EventPublisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// EventSubscriber defines event subscription port.
type EventSubscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan []byte, error)
}

// JobFilter represents criteria for filtering and paginating jobs.
type JobFilter struct {
	UserID    *uuid.UUID
	QueueID   *uuid.UUID
	WorkerID  *uuid.UUID
	Status    *job.Status
	TaskType  *string
	Limit     int
	Offset    int
	SortBy    string
	SortOrder string
}

// JobRepository defines the persistence contract for Job entities.
type JobRepository interface {
	Create(ctx context.Context, j *job.Job, outboxEvent *outbox.Event) error
	GetByID(ctx context.Context, id uuid.UUID) (*job.Job, error)
	List(ctx context.Context, filter JobFilter) ([]*job.Job, int64, error)
	ClaimNext(ctx context.Context, queueID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*job.Job, *job.JobAttempt, error)
	RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) error
	Complete(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error
	Fail(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error
	Cancel(ctx context.Context, jobID uuid.UUID) error
	RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*job.Job, error)
}

// JobAttemptRepository defines persistence for job attempt telemetry.
type JobAttemptRepository interface {
	Create(ctx context.Context, attempt *job.JobAttempt) error
	Update(ctx context.Context, attempt *job.JobAttempt) error
	ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*job.JobAttempt, error)
}

// UserRepository defines persistence for authentication and RBAC.
type UserRepository interface {
	Create(ctx context.Context, u *user.User) error
	GetByID(ctx context.Context, id uuid.UUID) (*user.User, error)
	GetByEmail(ctx context.Context, email string) (*user.User, error)
	Update(ctx context.Context, u *user.User) error
	AssignRole(ctx context.Context, userID uuid.UUID, role user.Role) error
}

// QueueRepository defines persistence for queues.
type QueueRepository interface {
	Create(ctx context.Context, q *queue.Queue) error
	GetByID(ctx context.Context, id uuid.UUID) (*queue.Queue, error)
	GetByName(ctx context.Context, name string) (*queue.Queue, error)
	List(ctx context.Context) ([]*queue.Queue, error)
	Update(ctx context.Context, q *queue.Queue) error
}

// WorkerRepository defines persistence for worker registration and telemetry.
type WorkerRepository interface {
	Register(ctx context.Context, w *worker.Worker) error
	Heartbeat(ctx context.Context, id uuid.UUID, now time.Time) error
	UpdateStatus(ctx context.Context, id uuid.UUID, status worker.Status) error
	GetByID(ctx context.Context, id uuid.UUID) (*worker.Worker, error)
	GetByKey(ctx context.Context, key string) (*worker.Worker, error)
	List(ctx context.Context) ([]*worker.Worker, error)
	FindDeadWorkers(ctx context.Context, threshold time.Duration) ([]*worker.Worker, error)
}

// OutboxRepository defines persistence for the transactional outbox pattern.
type OutboxRepository interface {
	Create(ctx context.Context, event *outbox.Event) error
	FetchPending(ctx context.Context, limit int) ([]*outbox.Event, error)
	MarkPublished(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID) error
}

// IdempotencyRecord stores idempotency payload cache.
type IdempotencyRecord struct {
	ID             uuid.UUID
	UserID         uuid.UUID
	Key            string
	RequestHash    string
	ResourceID     *uuid.UUID
	ResourceType   *string
	ResponseStatus *int
	ResponseBody   []byte
	ExpiresAt      *time.Time
	CreatedAt      time.Time
}

// IdempotencyRepository defines persistence for atomic idempotent request deduplication.
type IdempotencyRepository interface {
	GetOrLock(ctx context.Context, userID uuid.UUID, key, requestHash string, ttl time.Duration) (*IdempotencyRecord, bool, error)
	SaveResponse(ctx context.Context, userID uuid.UUID, key string, statusCode int, responseBody []byte, resourceID *uuid.UUID, resourceType *string) error
}
