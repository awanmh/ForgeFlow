package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/forgeflow/forgeflow/internal/infrastructure/config"
	"github.com/redis/go-redis/v9"
)

// Client wraps go-redis/v9 with lifecycle management and health checking.
type Client struct {
	UniversalClient redis.UniversalClient
	logger          *slog.Logger
}

// NewClient initializes a new Redis client based on configuration.
func NewClient(ctx context.Context, cfg config.RedisConfig, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.Default()
	}

	opt, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis connection url: %w", err)
	}

	opt.MaxRetries = cfg.MaxRetries
	opt.MinIdleConns = cfg.MinIdleConns
	opt.PoolSize = cfg.PoolSize
	opt.DialTimeout = cfg.DialTimeout
	opt.ReadTimeout = cfg.ReadTimeout
	opt.WriteTimeout = cfg.WriteTimeout

	rdb := redis.NewClient(opt)

	return &Client{
		UniversalClient: rdb,
		logger:          logger,
	}, nil
}

// Ping checks Redis connectivity with context timeout.
func (c *Client) Ping(ctx context.Context) error {
	if c.UniversalClient == nil {
		return fmt.Errorf("redis client is not initialized")
	}
	return c.UniversalClient.Ping(ctx).Err()
}

// Close gracefully closes the Redis client connections.
func (c *Client) Close() error {
	if c.UniversalClient != nil {
		c.logger.Info("closing redis client")
		return c.UniversalClient.Close()
	}
	return nil
}
