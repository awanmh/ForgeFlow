package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrEmptyQueueName = errors.New("queue name cannot be empty")
	ErrNilJobID       = errors.New("job ID cannot be nil")
)

// QueueMessage represents a dequeued job reference from Redis Streams.
type QueueMessage struct {
	ID        string    `json:"id"`
	JobID     uuid.UUID `json:"job_id"`
	Queue     string    `json:"queue"`
	Priority  int       `json:"priority"`
	Timestamp time.Time `json:"timestamp"`
}

// QueueEngine provides Redis Streams-backed job queue operations.
type QueueEngine struct {
	client *Client
	group  string
}

// NewQueueEngine initializes a new Redis Streams queue engine.
func NewQueueEngine(client *Client, consumerGroup string) *QueueEngine {
	if consumerGroup == "" {
		consumerGroup = "forgeflow-workers"
	}
	return &QueueEngine{
		client: client,
		group:  consumerGroup,
	}
}

func (q *QueueEngine) streamKey(queue string) string {
	return fmt.Sprintf("forgeflow:queue:%s", queue)
}

// EnsureGroup creates the consumer group and stream if they do not exist.
func (q *QueueEngine) EnsureGroup(ctx context.Context, queue string) error {
	key := q.streamKey(queue)
	err := q.client.UniversalClient.XGroupCreateMkStream(ctx, key, q.group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create redis consumer group: %w", err)
	}
	return nil
}

// Enqueue adds a job reference to the specified Redis Stream.
func (q *QueueEngine) Enqueue(ctx context.Context, queue string, jobID uuid.UUID, priority int) (string, error) {
	if queue == "" {
		return "", ErrEmptyQueueName
	}
	if jobID == uuid.Nil {
		return "", ErrNilJobID
	}

	key := q.streamKey(queue)
	values := map[string]any{
		"job_id":      jobID.String(),
		"priority":    priority,
		"enqueued_at": time.Now().UTC().Format(time.RFC3339Nano),
	}

	msgID, err := q.client.UniversalClient.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("failed to XADD job to stream %s: %w", key, err)
	}

	return msgID, nil
}

// Dequeue consumes up to `count` messages from the Redis Stream using consumer groups.
func (q *QueueEngine) Dequeue(ctx context.Context, queue, consumerName string, count int64, block time.Duration) ([]QueueMessage, error) {
	if queue == "" {
		return nil, ErrEmptyQueueName
	}
	if err := q.EnsureGroup(ctx, queue); err != nil {
		return nil, err
	}

	key := q.streamKey(queue)
	streams, err := q.client.UniversalClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumerName,
		Streams:  []string{key, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to XREADGROUP on stream %s: %w", key, err)
	}

	var messages []QueueMessage
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			jobIDStr, _ := msg.Values["job_id"].(string)
			jobID, parseErr := uuid.Parse(jobIDStr)
			if parseErr != nil {
				continue
			}

			priority := 0
			if p, ok := msg.Values["priority"].(int); ok {
				priority = p
			}

			messages = append(messages, QueueMessage{
				ID:        msg.ID,
				JobID:     jobID,
				Queue:     queue,
				Priority:  priority,
				Timestamp: time.Now().UTC(),
			})
		}
	}

	return messages, nil
}

// Ack acknowledges a message has been processed in Redis Streams.
func (q *QueueEngine) Ack(ctx context.Context, queue, messageID string) error {
	if queue == "" || messageID == "" {
		return nil
	}
	key := q.streamKey(queue)
	err := q.client.UniversalClient.XAck(ctx, key, q.group, messageID).Err()
	if err != nil {
		return fmt.Errorf("failed to XACK message %s on stream %s: %w", messageID, key, err)
	}
	return nil
}

// QueueDepth returns the total number of items in the Redis Stream.
func (q *QueueEngine) QueueDepth(ctx context.Context, queue string) (int64, error) {
	key := q.streamKey(queue)
	length, err := q.client.UniversalClient.XLen(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get XLEN for stream %s: %w", key, err)
	}
	return length, nil
}

// PendingCount returns the number of unacknowledged pending messages in the consumer group.
func (q *QueueEngine) PendingCount(ctx context.Context, queue string) (int64, error) {
	key := q.streamKey(queue)
	pending, err := q.client.UniversalClient.XPending(ctx, key, q.group).Result()
	if err != nil {
		if strings.Contains(err.Error(), "NOGROUP") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get XPENDING for stream %s: %w", key, err)
	}
	return pending.Count, nil
}
