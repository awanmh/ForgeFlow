package shutdown

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Hook represents a named cleanup action executed during shutdown.
type Hook struct {
	Name string
	Func func(ctx context.Context) error
}

// Manager coordinates graceful process termination across multiple subsystems.
type Manager struct {
	logger  *slog.Logger
	timeout time.Duration

	mu    sync.Mutex
	hooks []Hook
}

// NewManager creates a new graceful shutdown manager.
func NewManager(logger *slog.Logger, timeout time.Duration) *Manager {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		logger:  logger,
		timeout: timeout,
		hooks:   make([]Hook, 0),
	}
}

// Register adds a cleanup hook. Hooks are executed in LIFO order (last registered, first executed).
func (m *Manager) Register(name string, fn func(ctx context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks = append(m.hooks, Hook{
		Name: name,
		Func: fn,
	})
}

// Listen waits for an OS termination signal and executes all registered hooks within the timeout.
func (m *Manager) Listen(ctx context.Context) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigChan:
		m.logger.Info("termination signal received, initiating graceful shutdown", "signal", sig.String())
	case <-ctx.Done():
		m.logger.Info("context cancelled, initiating graceful shutdown")
	}

	return m.Execute()
}

// Execute runs all registered cleanup hooks in reverse order within the configured timeout.
func (m *Manager) Execute() error {
	m.mu.Lock()
	hooksToRun := make([]Hook, len(m.hooks))
	copy(hooksToRun, m.hooks)
	m.mu.Unlock()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	var errs []error

	// Execute in LIFO order
	for i := len(hooksToRun) - 1; i >= 0; i-- {
		hook := hooksToRun[i]
		m.logger.Info("executing shutdown hook", "hook", hook.Name)

		done := make(chan error, 1)
		go func(h Hook) {
			done <- h.Func(shutdownCtx)
		}(hook)

		select {
		case err := <-done:
			if err != nil {
				m.logger.Error("shutdown hook returned error", "hook", hook.Name, "error", err)
				errs = append(errs, fmt.Errorf("hook %s failed: %w", hook.Name, err))
			} else {
				m.logger.Info("shutdown hook completed successfully", "hook", hook.Name)
			}
		case <-shutdownCtx.Done():
			m.logger.Error("shutdown timeout exceeded while waiting for hook", "hook", hook.Name)
			errs = append(errs, fmt.Errorf("hook %s timed out", hook.Name))
			return fmt.Errorf("graceful shutdown timed out: %v", errs)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown completed with %d errors", len(errs))
	}

	m.logger.Info("graceful shutdown completed cleanly")
	return nil
}
