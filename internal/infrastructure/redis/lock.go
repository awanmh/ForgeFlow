package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/ports"
)

var (
	ErrLockAcquisitionFailed = errors.New("failed to acquire distributed lock: lock is held by another process")
	ErrLockNotHeld           = errors.New("distributed lock is not held or token mismatch")
)

const (
	// Lua script for atomic safe lock release verifying token ownership
	luaReleaseLock = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`

	// Lua script for atomic safe lock renewal verifying token ownership
	luaRenewLock = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
)

// Locker implements ports.Locker using Redis with token ownership safety.
type Locker struct {
	client *Client
}

// NewLocker constructs a new Redis distributed locker.
func NewLocker(client *Client) *Locker {
	return &Locker{client: client}
}

// Acquire attempts to acquire a named lock with the specified TTL.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (ports.Lock, error) {
	if key == "" {
		return nil, errors.New("lock key cannot be empty")
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}

	token := uuid.New().String()
	lockKey := fmt.Sprintf("forgeflow:lock:%s", key)

	ok, err := l.client.UniversalClient.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("redis error acquiring lock %s: %w", lockKey, err)
	}
	if !ok {
		return nil, ErrLockAcquisitionFailed
	}

	return &DistLock{
		client: l.client,
		key:    lockKey,
		token:  token,
	}, nil
}

// DistLock represents an active distributed lock held by the current process.
type DistLock struct {
	client *Client
	key    string
	token  string
}

func (d *DistLock) Token() string { return d.token }
func (d *DistLock) Key() string   { return d.key }

// Renew extends the lock TTL atomically if and only if the stored token matches.
func (d *DistLock) Renew(ctx context.Context, ttl time.Duration) error {
	res, err := d.client.UniversalClient.Eval(ctx, luaRenewLock, []string{d.key}, d.token, int64(ttl/time.Millisecond)).Result()
	if err != nil {
		return fmt.Errorf("failed to execute lua lock renewal: %w", err)
	}

	if count, ok := res.(int64); !ok || count == 0 {
		return ErrLockNotHeld
	}
	return nil
}

// Release deletes the lock atomically if and only if the stored token matches.
func (d *DistLock) Release(ctx context.Context) error {
	res, err := d.client.UniversalClient.Eval(ctx, luaReleaseLock, []string{d.key}, d.token).Result()
	if err != nil {
		return fmt.Errorf("failed to execute lua lock release: %w", err)
	}

	if count, ok := res.(int64); !ok || count == 0 {
		return ErrLockNotHeld
	}
	return nil
}
