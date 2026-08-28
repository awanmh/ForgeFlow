package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/forgeflow/forgeflow/internal/infrastructure/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client wraps a pgxpool.Pool with lifecycle management, health check, and transaction helpers.
type Client struct {
	Pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewClient initializes a new PostgreSQL connection pool.
func NewClient(ctx context.Context, cfg config.DatabaseConfig, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.Default()
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres connection url: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxOpenConns)
	poolConfig.MinConns = int32(cfg.MinOpenConns)
	poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	client := &Client{
		Pool:   pool,
		logger: logger,
	}

	return client, nil
}

// Ping checks database availability with the given timeout context.
func (c *Client) Ping(ctx context.Context) error {
	if c.Pool == nil {
		return fmt.Errorf("postgres pool is not initialized")
	}
	return c.Pool.Ping(ctx)
}

// Close gracefully terminates all connections in the pool.
func (c *Client) Close() {
	if c.Pool != nil {
		c.logger.Info("closing postgres connection pool")
		c.Pool.Close()
	}
}

// TxFunc is a function signature executed inside a PostgreSQL transaction.
type TxFunc func(ctx context.Context, tx pgx.Tx) error

// WithinTransaction executes a callback within a database transaction, automatically handling rollback on error or panic.
func (c *Client) WithinTransaction(ctx context.Context, fn TxFunc) (err error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p) // re-throw panic after rollback
		} else if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && rbErr != pgx.ErrTxClosed {
				c.logger.Error("failed to rollback transaction", "error", rbErr)
			}
		} else {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				err = fmt.Errorf("failed to commit transaction: %w", commitErr)
			}
		}
	}()

	err = fn(ctx, tx)
	return err
}

// Stats returns connection pool statistics for observability metrics.
type Stats struct {
	TotalConns int32
	IdleConns  int32
	Acquired   int32
}

// Stats returns current pool health statistics.
func (c *Client) Stats() Stats {
	if c.Pool == nil {
		return Stats{}
	}
	s := c.Pool.Stat()
	return Stats{
		TotalConns: s.TotalConns(),
		IdleConns:  s.IdleConns(),
		Acquired:   s.AcquiredConns(),
	}
}
