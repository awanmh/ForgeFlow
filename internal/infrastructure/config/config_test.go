package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_DefaultValues(t *testing.T) {
	// Clear relevant env vars
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("REDIS_URL")
	os.Unsetenv("HTTP_PORT")
	os.Unsetenv("ENV")

	cfg, err := Load()
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "development", cfg.Environment)
	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 10, cfg.Worker.Concurrency)
	assert.Equal(t, 30*time.Second, cfg.Worker.LeaseDuration)
	assert.Equal(t, 10*time.Second, cfg.Worker.HeartbeatInterval)
}

func TestConfig_EnvironmentOverride(t *testing.T) {
	t.Setenv("HTTP_PORT", "9000")
	t.Setenv("WORKER_CONCURRENCY", "32")
	t.Setenv("LEASE_DURATION", "45s")
	t.Setenv("HEARTBEAT_INTERVAL", "15s")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 9000, cfg.Server.Port)
	assert.Equal(t, 32, cfg.Worker.Concurrency)
	assert.Equal(t, 45*time.Second, cfg.Worker.LeaseDuration)
	assert.Equal(t, 15*time.Second, cfg.Worker.HeartbeatInterval)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(c *AppConfig)
		expectError string
	}{
		{
			name: "empty database URL",
			modify: func(c *AppConfig) {
				c.Database.URL = ""
			},
			expectError: "DATABASE_URL must not be empty",
		},
		{
			name: "empty redis URL",
			modify: func(c *AppConfig) {
				c.Redis.URL = ""
			},
			expectError: "REDIS_URL must not be empty",
		},
		{
			name: "invalid port",
			modify: func(c *AppConfig) {
				c.Server.Port = 70000
			},
			expectError: "invalid HTTP_PORT",
		},
		{
			name: "zero concurrency",
			modify: func(c *AppConfig) {
				c.Worker.Concurrency = 0
			},
			expectError: "WORKER_CONCURRENCY must be greater than 0",
		},
		{
			name: "heartbeat greater than or equal to lease",
			modify: func(c *AppConfig) {
				c.Worker.LeaseDuration = 10 * time.Second
				c.Worker.HeartbeatInterval = 10 * time.Second
			},
			expectError: "HEARTBEAT_INTERVAL (10s) must be strictly less than LEASE_DURATION (10s)",
		},
		{
			name: "invalid retry backoff multiplier",
			modify: func(c *AppConfig) {
				c.Worker.BackoffMultiplier = 0.5
			},
			expectError: "RETRY_BACKOFF_MULTIPLIER must be greater than 1.0",
		},
		{
			name: "production with default secret",
			modify: func(c *AppConfig) {
				c.Environment = "production"
				c.Auth.JWTSecret = "forgeflow-insecure-dev-secret-change-in-production"
			},
			expectError: "JWT_SECRET must be securely configured in production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load()
			require.NoError(t, err)

			tt.modify(cfg)
			valErr := cfg.Validate()
			require.Error(t, valErr)
			assert.Contains(t, valErr.Error(), tt.expectError)
		})
	}
}
