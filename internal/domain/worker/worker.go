package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrWorkerNotFound = errors.New("worker not found")
	ErrInvalidWorker  = errors.New("invalid worker configuration")
)

type Status string

const (
	StatusStarting Status = "STARTING"
	StatusActive   Status = "ACTIVE"
	StatusDraining Status = "DRAINING"
	StatusStopped  Status = "STOPPED"
	StatusDead     Status = "DEAD"
)

type Worker struct {
	ID              uuid.UUID       `json:"id"`
	WorkerKey       string          `json:"worker_key"`
	Hostname        string          `json:"hostname"`
	Version         string          `json:"version"`
	Status          Status          `json:"status"`
	Concurrency     int             `json:"concurrency"`
	RegisteredAt    time.Time       `json:"registered_at"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at,omitempty"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	StoppedAt       *time.Time      `json:"stopped_at,omitempty"`
	Capabilities    json.RawMessage `json:"capabilities"`
	Metadata        json.RawMessage `json:"metadata"`
}

func NewWorker(workerKey, hostname, version string, concurrency int, capabilities []string) (*Worker, error) {
	if workerKey == "" {
		return nil, fmt.Errorf("%w: worker_key is required", ErrInvalidWorker)
	}
	if concurrency <= 0 {
		return nil, fmt.Errorf("%w: concurrency must be > 0", ErrInvalidWorker)
	}

	capJSON, err := json.Marshal(capabilities)
	if err != nil {
		capJSON = []byte("[]")
	}

	now := time.Now().UTC()
	return &Worker{
		ID:              uuid.New(),
		WorkerKey:       workerKey,
		Hostname:        hostname,
		Version:         version,
		Status:          StatusStarting,
		Concurrency:     concurrency,
		RegisteredAt:    now,
		LastHeartbeatAt: &now,
		Capabilities:    capJSON,
		Metadata:        []byte("{}"),
	}, nil
}

func (w *Worker) SetActive(now time.Time) {
	nowUTC := now.UTC()
	w.Status = StatusActive
	w.StartedAt = &nowUTC
	w.LastHeartbeatAt = &nowUTC
}

func (w *Worker) Heartbeat(now time.Time) {
	nowUTC := now.UTC()
	w.LastHeartbeatAt = &nowUTC
}

func (w *Worker) SetDraining() {
	w.Status = StatusDraining
}

func (w *Worker) SetStopped(now time.Time) {
	nowUTC := now.UTC()
	w.Status = StatusStopped
	w.StoppedAt = &nowUTC
}
