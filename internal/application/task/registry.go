package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

var (
	ErrTaskTypeNotFound  = errors.New("task type not registered")
	ErrTaskExecutionFail = errors.New("task execution failed")
)

// Handler defines the contract for an executable task handler.
type Handler interface {
	Type() string
	Execute(ctx context.Context, payload []byte) error
}

// Registry manages task handlers available to workers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry initializes a registry with standard built-in handlers.
func NewRegistry() *Registry {
	r := &Registry{
		handlers: make(map[string]Handler),
	}

	r.Register(&HTTPHandler{})
	r.Register(&NotificationHandler{})
	r.Register(&BackupHandler{})
	r.Register(&DockerBuildHandler{})
	r.Register(&DemoHandler{})

	return r
}

// Register registers a new task handler for a specific task type.
func (r *Registry) Register(h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[h.Type()] = h
}

// Get retrieves a task handler by task type name.
func (r *Registry) Get(taskType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[taskType]
	return h, ok
}

// RegisteredTypes returns a slice of all registered task names.
func (r *Registry) RegisteredTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// --- Built-in Handlers ---

// HTTPHandler executes outbound HTTP requests.
type HTTPHandler struct{}

func (h *HTTPHandler) Type() string { return "http-request" }

type HTTPPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	Timeout int               `json:"timeout_seconds"`
}

func (h *HTTPHandler) Execute(ctx context.Context, payload []byte) error {
	var p HTTPPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid http task payload: %w", err)
	}
	if p.URL == "" {
		return errors.New("url is required for http task")
	}
	if p.Method == "" {
		p.Method = http.MethodGet
	}

	reqTimeout := 30 * time.Second
	if p.Timeout > 0 {
		reqTimeout = time.Duration(p.Timeout) * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
	defer cancel()

	var bodyReader io.Reader
	if p.Body != "" {
		bodyReader = bytes.NewBufferString(p.Body)
	}

	req, err := http.NewRequestWithContext(reqCtx, p.Method, p.URL, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: reqTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http request returned error status: %d", resp.StatusCode)
	}

	return nil
}

// NotificationHandler delivers notifications/webhooks.
type NotificationHandler struct{}

func (n *NotificationHandler) Type() string { return "notification" }

type NotificationPayload struct {
	Channel string `json:"channel"`
	Target  string `json:"target"`
	Message string `json:"message"`
}

func (n *NotificationHandler) Execute(ctx context.Context, payload []byte) error {
	var p NotificationPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid notification payload: %w", err)
	}
	if p.Message == "" {
		return errors.New("notification message cannot be empty")
	}
	// Simulated notification delivery
	time.Sleep(10 * time.Millisecond)
	return nil
}

// BackupHandler simulates database backups.
type BackupHandler struct{}

func (b *BackupHandler) Type() string { return "database-backup" }

type BackupPayload struct {
	Database string `json:"database"`
	TargetS3 string `json:"target_s3"`
}

func (b *BackupHandler) Execute(ctx context.Context, payload []byte) error {
	var p BackupPayload
	_ = json.Unmarshal(payload, &p)
	// Simulated backup execution
	time.Sleep(20 * time.Millisecond)
	return nil
}

// DockerBuildHandler simulates container builds.
type DockerBuildHandler struct{}

func (d *DockerBuildHandler) Type() string { return "docker-build" }

type DockerBuildPayload struct {
	ImageTag   string `json:"image_tag"`
	Dockerfile string `json:"dockerfile"`
}

func (d *DockerBuildHandler) Execute(ctx context.Context, payload []byte) error {
	var p DockerBuildPayload
	_ = json.Unmarshal(payload, &p)
	// Simulated build step
	time.Sleep(25 * time.Millisecond)
	return nil
}

// DemoHandler is a versatile testing handler that can simulate delays, errors, and panics.
type DemoHandler struct{}

func (d *DemoHandler) Type() string { return "custom-demo" }

type DemoPayload struct {
	Action      string `json:"action"` // "success", "error", "panic", "sleep"
	SleepMillis int    `json:"sleep_millis"`
	ErrorMsg    string `json:"error_msg"`
}

func (d *DemoHandler) Execute(ctx context.Context, payload []byte) error {
	var p DemoPayload
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}

	if p.SleepMillis > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMillis) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	switch p.Action {
	case "panic":
		panic("simulated worker task panic")
	case "error":
		if p.ErrorMsg != "" {
			return errors.New(p.ErrorMsg)
		}
		return errors.New("simulated task error")
	default:
		return nil
	}
}
