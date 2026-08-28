package postgres

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/forgeflow/forgeflow/internal/infrastructure/config"
	"github.com/stretchr/testify/assert"
)

func TestPostgres_InvalidConfig(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := config.DatabaseConfig{
		URL: "invalid://postgres URL with spaces",
	}

	client, err := NewClient(context.Background(), cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestPostgres_PingUninitialized(t *testing.T) {
	client := &Client{}
	err := client.Ping(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestPostgres_StatsUninitialized(t *testing.T) {
	client := &Client{}
	stats := client.Stats()
	assert.Equal(t, int32(0), stats.TotalConns)
}
