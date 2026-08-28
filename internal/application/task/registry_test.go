package task_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/application/task"
)

func TestTaskRegistry_StandardHandlers(t *testing.T) {
	reg := task.NewRegistry()
	types := reg.RegisteredTypes()

	assert.Contains(t, types, "http-request")
	assert.Contains(t, types, "notification")
	assert.Contains(t, types, "database-backup")
	assert.Contains(t, types, "docker-build")
	assert.Contains(t, types, "custom-demo")
}

func TestDemoHandler_ExecutionModes(t *testing.T) {
	reg := task.NewRegistry()
	handler, ok := reg.Get("custom-demo")
	require.True(t, ok)

	ctx := context.Background()

	t.Run("success mode", func(t *testing.T) {
		err := handler.Execute(ctx, []byte(`{"action":"success"}`))
		assert.NoError(t, err)
	})

	t.Run("error mode", func(t *testing.T) {
		err := handler.Execute(ctx, []byte(`{"action":"error","error_msg":"custom failure"}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "custom failure")
	})

	t.Run("panic mode", func(t *testing.T) {
		assert.Panics(t, func() {
			_ = handler.Execute(ctx, []byte(`{"action":"panic"}`))
		})
	})
}

func TestNotificationHandler_Execution(t *testing.T) {
	reg := task.NewRegistry()
	handler, ok := reg.Get("notification")
	require.True(t, ok)

	ctx := context.Background()

	t.Run("valid notification", func(t *testing.T) {
		err := handler.Execute(ctx, []byte(`{"channel":"slack","target":"#deployments","message":"Deploy completed"}`))
		assert.NoError(t, err)
	})

	t.Run("empty message error", func(t *testing.T) {
		err := handler.Execute(ctx, []byte(`{"channel":"slack","target":"#deployments"}`))
		assert.Error(t, err)
	})
}
