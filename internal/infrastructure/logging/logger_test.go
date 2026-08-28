package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogger_SecretRedaction(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(Config{
		Level:  "info",
		Writer: buf,
	})

	logger.Info("user login attempt",
		"email", "developer@forgeflow.internal",
		"password", "super-secret-password-123",
		"token", "bearer-token-xyz",
	)

	output := buf.String()
	assert.Contains(t, output, "developer@forgeflow.internal")
	assert.NotContains(t, output, "super-secret-password-123")
	assert.NotContains(t, output, "bearer-token-xyz")
	assert.Contains(t, output, "[REDACTED]")

	var logMap map[string]any
	err := json.Unmarshal(buf.Bytes(), &logMap)
	require.NoError(t, err)
	assert.Equal(t, "[REDACTED]", logMap["password"])
	assert.Equal(t, "[REDACTED]", logMap["token"])
}

func TestLogger_ContextCorrelation(t *testing.T) {
	buf := &bytes.Buffer{}
	baseLogger := New(Config{
		Level:  "debug",
		Writer: buf,
	})

	ctx := context.Background()
	ctx = WithRequestID(ctx, "req-12345")
	ctx = WithJobID(ctx, "job-67890")
	ctx = WithWorkflowID(ctx, "wf-abcde")
	ctx = WithWorkerID(ctx, "worker-primary-01")

	corrLogger := FromContext(ctx, baseLogger)
	corrLogger.Info("job execution started", "attempt", 1)

	var logMap map[string]any
	err := json.Unmarshal(buf.Bytes(), &logMap)
	require.NoError(t, err)

	assert.Equal(t, "req-12345", logMap["request_id"])
	assert.Equal(t, "job-67890", logMap["job_id"])
	assert.Equal(t, "wf-abcde", logMap["workflow_id"])
	assert.Equal(t, "worker-primary-01", logMap["worker_id"])
	assert.Equal(t, float64(1), logMap["attempt"])
	assert.Equal(t, "job execution started", logMap["msg"])
}
