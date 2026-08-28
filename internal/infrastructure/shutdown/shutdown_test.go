package shutdown

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownManager_LIFOExecution(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr := NewManager(logger, 5*time.Second)

	var executionOrder []string

	mgr.Register("DatabasePool", func(ctx context.Context) error {
		executionOrder = append(executionOrder, "DatabasePool")
		return nil
	})

	mgr.Register("RedisClient", func(ctx context.Context) error {
		executionOrder = append(executionOrder, "RedisClient")
		return nil
	})

	mgr.Register("HTTPServer", func(ctx context.Context) error {
		executionOrder = append(executionOrder, "HTTPServer")
		return nil
	})

	err := mgr.Execute()
	require.NoError(t, err)

	// Expected LIFO order: HTTPServer -> RedisClient -> DatabasePool
	assert.Equal(t, []string{"HTTPServer", "RedisClient", "DatabasePool"}, executionOrder)
}

func TestShutdownManager_ErrorHandling(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr := NewManager(logger, 5*time.Second)

	mgr.Register("CleanHook", func(ctx context.Context) error {
		return nil
	})

	mgr.Register("FailingHook", func(ctx context.Context) error {
		return errors.New("connection failed during close")
	})

	err := mgr.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown completed with 1 errors")
}

func TestShutdownManager_Timeout(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr := NewManager(logger, 50*time.Millisecond)

	mgr.Register("HangingHook", func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	err := mgr.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}
