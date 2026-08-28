package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type contextKey string

const (
	RequestIDKey  contextKey = "request_id"
	TraceIDKey    contextKey = "trace_id"
	JobIDKey      contextKey = "job_id"
	WorkflowIDKey contextKey = "workflow_id"
	WorkerIDKey   contextKey = "worker_id"
)

var sensitiveKeys = map[string]bool{
	"password":      true,
	"password_hash": true,
	"secret":        true,
	"token":         true,
	"jwt":           true,
	"authorization": true,
	"auth_header":   true,
	"api_key":       true,
	"apikey":        true,
}

// Config specifies logger configuration.
type Config struct {
	Level  string
	Writer io.Writer
}

// New creates a production structured slog.Logger with sensitive key masking.
func New(cfg Config) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(cfg.Level)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	w := cfg.Writer
	if w == nil {
		w = os.Stdout
	}

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Redact sensitive keys
			keyLower := strings.ToLower(a.Key)
			if sensitiveKeys[keyLower] {
				return slog.String(a.Key, "[REDACTED]")
			}
			return a
		},
	})

	return slog.New(handler)
}

// WithRequestID returns a new context with the given request ID.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDKey, requestID)
}

// WithJobID returns a new context with the given job ID.
func WithJobID(ctx context.Context, jobID string) context.Context {
	return context.WithValue(ctx, JobIDKey, jobID)
}

// WithWorkflowID returns a new context with the given workflow ID.
func WithWorkflowID(ctx context.Context, workflowID string) context.Context {
	return context.WithValue(ctx, WorkflowIDKey, workflowID)
}

// WithWorkerID returns a new context with the given worker ID.
func WithWorkerID(ctx context.Context, workerID string) context.Context {
	return context.WithValue(ctx, WorkerIDKey, workerID)
}

// FromContext extracts contextual identifiers from context and returns a correlated logger.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if ctx == nil {
		return base
	}

	var attrs []any

	if reqID, ok := ctx.Value(RequestIDKey).(string); ok && reqID != "" {
		attrs = append(attrs, slog.String("request_id", reqID))
	}
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		attrs = append(attrs, slog.String("trace_id", traceID))
	}
	if jobID, ok := ctx.Value(JobIDKey).(string); ok && jobID != "" {
		attrs = append(attrs, slog.String("job_id", jobID))
	}
	if wfID, ok := ctx.Value(WorkflowIDKey).(string); ok && wfID != "" {
		attrs = append(attrs, slog.String("workflow_id", wfID))
	}
	if wID, ok := ctx.Value(WorkerIDKey).(string); ok && wID != "" {
		attrs = append(attrs, slog.String("worker_id", wID))
	}

	if len(attrs) == 0 {
		return base
	}

	return base.With(attrs...)
}
