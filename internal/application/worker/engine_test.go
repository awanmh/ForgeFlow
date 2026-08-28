package worker_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/forgeflow/forgeflow/internal/application/task"
	"github.com/forgeflow/forgeflow/internal/application/worker"
)

func TestWorkerEngine_CalculateBackoff(t *testing.T) {
	cfg := worker.Config{
		WorkerID:          uuid.New(),
		BackoffInitial:    1 * time.Second,
		BackoffMax:        30 * time.Second,
		BackoffMultiplier: 2.0,
		JitterPercent:     0.20, // +/- 20%
	}

	reg := task.NewRegistry()
	engine := worker.NewEngine(cfg, nil, nil, nil, reg, nil)

	t.Run("attempt 1 backoff within jitter range", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			b := engine.CalculateBackoff(1)
			assert.GreaterOrEqual(t, b, 800*time.Millisecond)
			assert.LessOrEqual(t, b, 1200*time.Millisecond)
		}
	})

	t.Run("attempt 2 backoff within jitter range", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			b := engine.CalculateBackoff(2)
			assert.GreaterOrEqual(t, b, 1600*time.Millisecond)
			assert.LessOrEqual(t, b, 2400*time.Millisecond)
		}
	})

	t.Run("attempt 3 backoff within jitter range", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			b := engine.CalculateBackoff(3)
			assert.GreaterOrEqual(t, b, 3200*time.Millisecond)
			assert.LessOrEqual(t, b, 4800*time.Millisecond)
		}
	})

	t.Run("max backoff cap", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			b := engine.CalculateBackoff(10)
			assert.LessOrEqual(t, b, 36*time.Second) // 30s + 20%
		}
	})
}
