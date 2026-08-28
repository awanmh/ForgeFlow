package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/forgeflow/forgeflow/internal/infrastructure/config"
	"github.com/stretchr/testify/assert"
)

func TestRedis_InvalidConfig(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := config.RedisConfig{
		URL: "invalid-redis-url-without-scheme",
	}

	client, err := NewClient(context.Background(), cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestRedis_PingUninitialized(t *testing.T) {
	client := &Client{}
	err := client.Ping(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}
