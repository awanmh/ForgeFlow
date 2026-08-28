package queue

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	ErrInvalidQueue  = errors.New("invalid queue configuration")
)

type Queue struct {
	ID             uuid.UUID `json:"id"`
	Name           string    `json:"name"`
	Description    *string   `json:"description,omitempty"`
	MaxConcurrency *int      `json:"max_concurrency,omitempty"`
	PriorityLevels int       `json:"priority_levels"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func NewQueue(name string, description *string, maxConcurrency *int, priorityLevels int) (*Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidQueue)
	}
	if priorityLevels <= 0 || priorityLevels > 100 {
		priorityLevels = 10
	}
	if maxConcurrency != nil && *maxConcurrency <= 0 {
		return nil, fmt.Errorf("%w: max_concurrency must be positive", ErrInvalidQueue)
	}

	now := time.Now().UTC()
	return &Queue{
		ID:             uuid.New(),
		Name:           name,
		Description:    description,
		MaxConcurrency: maxConcurrency,
		PriorityLevels: priorityLevels,
		Enabled:        true,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}
