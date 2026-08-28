package ports

import (
	"context"
	"time"
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

// IDGenerator defines a unique identifier generator abstraction.
type IDGenerator interface {
	Generate() string
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
