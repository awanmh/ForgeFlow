package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusPublished Status = "PUBLISHED"
	StatusFailed    Status = "FAILED"
)

type Event struct {
	ID            uuid.UUID       `json:"id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
	Status        Status          `json:"status"`
	Attempts      int             `json:"attempts"`
	AvailableAt   time.Time       `json:"available_at"`
	CreatedAt     time.Time       `json:"created_at"`
	PublishedAt   *time.Time      `json:"published_at,omitempty"`
}

func NewEvent(eventType, aggregateType string, aggregateID uuid.UUID, payload any) (*Event, error) {
	var payloadBytes []byte
	var err error

	if raw, ok := payload.([]byte); ok {
		payloadBytes = raw
	} else if rawStr, ok := payload.(string); ok {
		payloadBytes = []byte(rawStr)
	} else {
		payloadBytes, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	now := time.Now().UTC()
	return &Event{
		ID:            uuid.New(),
		EventType:     eventType,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Payload:       payloadBytes,
		Status:        StatusPending,
		Attempts:      0,
		AvailableAt:   now,
		CreatedAt:     now,
	}, nil
}
