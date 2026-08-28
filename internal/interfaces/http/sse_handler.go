package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	goRedis "github.com/redis/go-redis/v9"

	"github.com/forgeflow/forgeflow/internal/infrastructure/redis"
)

type SSEHandler struct {
	rdb *redis.Client
}

func NewSSEHandler(rdb *redis.Client) *SSEHandler {
	return &SSEHandler{rdb: rdb}
}

// HandleStream provides a real-time Server-Sent Events stream of system events.
func (h *SSEHandler) HandleStream(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	rc := http.NewResponseController(c.Writer)
	_ = rc.SetWriteDeadline(time.Time{})

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streaming unsupported"})
		return
	}

	// Send initial connection event
	_, _ = fmt.Fprintf(c.Writer, "event: connected\ndata: {\"status\":\"connected\",\"timestamp\":\"%s\"}\n\n", time.Now().UTC().Format(time.RFC3339))
	flusher.Flush()

	ctx := c.Request.Context()
	var msgChan <-chan *goRedis.Message
	if h.rdb != nil {
		pubsub := h.rdb.UniversalClient.Subscribe(ctx, "forgeflow:events")
		defer pubsub.Close()
		msgChan = pubsub.Channel()
	}

	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-msgChan:
			if !ok {
				return
			}
			_, err := fmt.Fprintf(c.Writer, "event: message\ndata: %s\n\n", msg.Payload)
			if err != nil {
				return
			}
			flusher.Flush()

		case <-heartbeatTicker.C:
			_, err := fmt.Fprintf(c.Writer, ": keep-alive\n\n")
			if err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// SendEvent helper publishes an event to the Redis event channel.
func SendEvent(rdb *redis.Client, eventType string, payload []byte) {
	if rdb == nil {
		return
	}
	msg := fmt.Sprintf(`{"type":"%s","data":%s,"timestamp":"%s"}`, eventType, string(payload), time.Now().UTC().Format(time.RFC3339))
	_ = rdb.UniversalClient.Publish(context.Background(), "forgeflow:events", msg).Err()
}
