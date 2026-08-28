package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AppConfig represents the strongly-typed root configuration for ForgeFlow services.
type AppConfig struct {
	Environment string
	LogLevel    string

	Server     ServerConfig
	Database   DatabaseConfig
	Redis      RedisConfig
	Worker     WorkerConfig
	Scheduler  SchedulerConfig
	Auth       AuthConfig
	Monitoring MonitoringConfig
}

type ServerConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type DatabaseConfig struct {
	URL             string
	MaxOpenConns    int
	MinOpenConns    int
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	HealthTimeout   time.Duration
}

type RedisConfig struct {
	URL          string
	MaxRetries   int
	MinIdleConns int
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type WorkerConfig struct {
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	DefaultTimeout    time.Duration
	MaxRetries        int
	BackoffInitial    time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64
	JitterPercent     float64
}

type SchedulerConfig struct {
	PollInterval          time.Duration
	LeaseRecoveryInterval time.Duration
	WorkflowCheckInterval time.Duration
	LeaderLockTTL         time.Duration
}

type AuthConfig struct {
	JWTSecret       string
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

type MonitoringConfig struct {
	MetricsPort int
	MetricsPath string
}

// Load loads and validates configuration from environment variables with fallback defaults.
func Load() (*AppConfig, error) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "forgeflow-worker"
	}

	cfg := &AppConfig{
		Environment: getEnv("ENV", "development"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		Server: ServerConfig{
			Port:         getEnvAsInt("HTTP_PORT", 8080),
			ReadTimeout:  getEnvAsDuration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getEnvAsDuration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:  getEnvAsDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		},

		Database: DatabaseConfig{
			URL:             getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/forgeflow?sslmode=disable"),
			MaxOpenConns:    getEnvAsInt("DB_MAX_OPEN_CONNS", 25),
			MinOpenConns:    getEnvAsInt("DB_MIN_OPEN_CONNS", 5),
			MaxConnLifetime: getEnvAsDuration("DB_MAX_CONN_LIFETIME", 1*time.Hour),
			MaxConnIdleTime: getEnvAsDuration("DB_MAX_CONN_IDLE_TIME", 15*time.Minute),
			HealthTimeout:   getEnvAsDuration("DB_HEALTH_TIMEOUT", 5*time.Second),
		},

		Redis: RedisConfig{
			URL:          getEnv("REDIS_URL", "redis://localhost:6379/0"),
			MaxRetries:   getEnvAsInt("REDIS_MAX_RETRIES", 3),
			MinIdleConns: getEnvAsInt("REDIS_MIN_IDLE_CONNS", 5),
			PoolSize:     getEnvAsInt("REDIS_POOL_SIZE", 50),
			DialTimeout:  getEnvAsDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  getEnvAsDuration("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: getEnvAsDuration("REDIS_WRITE_TIMEOUT", 3*time.Second),
		},

		Worker: WorkerConfig{
			WorkerID:          getEnv("WORKER_ID", fmt.Sprintf("%s-%d", hostname, os.Getpid())),
			Concurrency:       getEnvAsInt("WORKER_CONCURRENCY", 10),
			PollInterval:      getEnvAsDuration("WORKER_POLL_INTERVAL", 100*time.Millisecond),
			LeaseDuration:     getEnvAsDuration("LEASE_DURATION", 30*time.Second),
			HeartbeatInterval: getEnvAsDuration("HEARTBEAT_INTERVAL", 10*time.Second),
			DefaultTimeout:    getEnvAsDuration("JOB_DEFAULT_TIMEOUT", 60*time.Second),
			MaxRetries:        getEnvAsInt("JOB_MAX_RETRIES", 3),
			BackoffInitial:    getEnvAsDuration("RETRY_BACKOFF_INITIAL", 1*time.Second),
			BackoffMax:        getEnvAsDuration("RETRY_BACKOFF_MAX", 60*time.Second),
			BackoffMultiplier: getEnvAsFloat("RETRY_BACKOFF_MULTIPLIER", 2.0),
			JitterPercent:     getEnvAsFloat("RETRY_JITTER_PERCENT", 0.20),
		},

		Scheduler: SchedulerConfig{
			PollInterval:          getEnvAsDuration("SCHEDULER_POLL_INTERVAL", 1*time.Second),
			LeaseRecoveryInterval: getEnvAsDuration("SCHEDULER_LEASE_RECOVERY_INTERVAL", 5*time.Second),
			WorkflowCheckInterval: getEnvAsDuration("SCHEDULER_WORKFLOW_CHECK_INTERVAL", 2*time.Second),
			LeaderLockTTL:         getEnvAsDuration("SCHEDULER_LEADER_LOCK_TTL", 15*time.Second),
		},

		Auth: AuthConfig{
			JWTSecret:       getEnv("JWT_SECRET", "forgeflow-insecure-dev-secret-change-in-production"),
			AccessTokenTTL:  getEnvAsDuration("JWT_ACCESS_TOKEN_TTL", 15*time.Minute),
			RefreshTokenTTL: getEnvAsDuration("JWT_REFRESH_TOKEN_TTL", 7*24*time.Hour),
		},

		Monitoring: MonitoringConfig{
			MetricsPort: getEnvAsInt("METRICS_PORT", 9090),
			MetricsPath: getEnv("METRICS_PATH", "/metrics"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// Validate ensures critical configuration fields meet system invariants.
func (c *AppConfig) Validate() error {
	if strings.TrimSpace(c.Database.URL) == "" {
		return fmt.Errorf("DATABASE_URL must not be empty")
	}
	if strings.TrimSpace(c.Redis.URL) == "" {
		return fmt.Errorf("REDIS_URL must not be empty")
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid HTTP_PORT: %d", c.Server.Port)
	}
	if c.Worker.Concurrency <= 0 {
		return fmt.Errorf("WORKER_CONCURRENCY must be greater than 0, got %d", c.Worker.Concurrency)
	}
	if c.Worker.HeartbeatInterval >= c.Worker.LeaseDuration {
		return fmt.Errorf("HEARTBEAT_INTERVAL (%v) must be strictly less than LEASE_DURATION (%v)", c.Worker.HeartbeatInterval, c.Worker.LeaseDuration)
	}
	if c.Worker.BackoffMultiplier <= 1.0 {
		return fmt.Errorf("RETRY_BACKOFF_MULTIPLIER must be greater than 1.0, got %f", c.Worker.BackoffMultiplier)
	}
	if c.Worker.JitterPercent < 0 || c.Worker.JitterPercent > 1.0 {
		return fmt.Errorf("RETRY_JITTER_PERCENT must be between 0.0 and 1.0, got %f", c.Worker.JitterPercent)
	}
	if c.Environment == "production" && (c.Auth.JWTSecret == "" || c.Auth.JWTSecret == "forgeflow-insecure-dev-secret-change-in-production") {
		return fmt.Errorf("JWT_SECRET must be securely configured in production")
	}
	return nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

func getEnvAsFloat(key string, defaultVal float64) float64 {
	valStr := getEnv(key, "")
	if val, err := strconv.ParseFloat(valStr, 64); err == nil {
		return val
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	valStr := getEnv(key, "")
	if val, err := time.ParseDuration(valStr); err == nil {
		return val
	}
	return defaultVal
}
