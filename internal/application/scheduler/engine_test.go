package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/forgeflow/forgeflow/internal/application/scheduler"
)

func TestSchedulerEngine_Lifecycle(t *testing.T) {
	cfg := scheduler.Config{
		LeaderLockKey:    "test-leader",
		LeaderLockTTL:    5 * time.Second,
		RecoveryInterval: 50 * time.Millisecond,
		OutboxInterval:   50 * time.Millisecond,
	}

	engine := scheduler.NewEngine(cfg, nil, nil, nil, nil, nil, nil)
	assert.False(t, engine.IsLeader())

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	// In standalone mode (nil locker), engine self-assigns leader
	go func() {
		_ = engine.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	assert.True(t, engine.IsLeader())

	engine.Stop()
}
