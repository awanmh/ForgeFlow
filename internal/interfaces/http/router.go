package http

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/forgeflow/forgeflow/internal/infrastructure/logging"
	"github.com/forgeflow/forgeflow/internal/infrastructure/postgres"
	"github.com/forgeflow/forgeflow/internal/infrastructure/redis"
)

// Server encapsulates the HTTP handler, Gin engine, and dependencies.
type Server struct {
	Engine     *gin.Engine
	pgClient   *postgres.Client
	rdb        *redis.Client
	logger     *slog.Logger
	jobHandler *JobHandler
}

// RouterOptions allows injecting domain handlers into the router.
type RouterOptions struct {
	JobHandler *JobHandler
}

// NewRouter constructs the configured HTTP router with standard middleware and core system endpoints.
func NewRouter(pgClient *postgres.Client, rdb *redis.Client, logger *slog.Logger, opts ...RouterOptions) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	server := &Server{
		Engine:   engine,
		pgClient: pgClient,
		rdb:      rdb,
		logger:   logger,
	}

	if len(opts) > 0 {
		server.jobHandler = opts[0].JobHandler
	}

	// Global Middlewares
	engine.Use(server.requestIDMiddleware())
	engine.Use(server.structuredLoggerMiddleware())
	engine.Use(server.recoveryMiddleware())
	engine.Use(server.securityHeadersMiddleware())
	engine.Use(server.corsMiddleware())

	// Core API v1 routes
	v1 := engine.Group("/api/v1")
	{
		v1.GET("/health", server.handleHealth)
		v1.GET("/ready", server.handleReady)
		v1.GET("/metrics", gin.WrapH(promhttp.Handler()))

		if server.jobHandler != nil {
			jobs := v1.Group("/jobs")
			{
				jobs.POST("", server.jobHandler.HandleSubmitJob)
				jobs.GET("", server.jobHandler.HandleListJobs)
				jobs.GET("/:id", server.jobHandler.HandleGetJob)
				jobs.POST("/:id/cancel", server.jobHandler.HandleCancelJob)
			}
		}
	}

	return server
}

// requestIDMiddleware ensures every incoming request has an X-Request-ID header and context value.
func (s *Server) requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader("X-Request-ID")
		if reqID == "" {
			reqID = uuid.New().String()
		}
		c.Header("X-Request-ID", reqID)
		ctx := logging.WithRequestID(c.Request.Context(), reqID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// structuredLoggerMiddleware logs request metadata as structured JSON.
func (s *Server) structuredLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()
		reqLogger := logging.FromContext(c.Request.Context(), s.logger)

		reqLogger.Info("http_request",
			"method", method,
			"path", path,
			"status", status,
			"duration_ms", duration.Milliseconds(),
			"client_ip", c.ClientIP(),
		)
	}
}

// recoveryMiddleware catches panics and returns clean 500 JSON errors.
func (s *Server) recoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				reqLogger := logging.FromContext(c.Request.Context(), s.logger)
				reqLogger.Error("panic recovered in http handler", "error", r)

				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"code":       "INTERNAL_SERVER_ERROR",
						"message":    "An unexpected error occurred",
						"request_id": c.Writer.Header().Get("X-Request-ID"),
					},
				})
			}
		}()
		c.Next()
	}
}

// securityHeadersMiddleware injects standard defense headers.
func (s *Server) securityHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("X-XSS-Protection", "1; mode=block")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Next()
	}
}

// corsMiddleware sets minimal safe CORS headers.
func (s *Server) corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-Request-ID, Idempotency-Key")
		c.Header("Access-Control-Expose-Headers", "X-Request-ID, X-Idempotency-Replay")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "UP",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleReady(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	pgStatus := "UP"
	if s.pgClient != nil {
		if err := s.pgClient.Ping(ctx); err != nil {
			pgStatus = "DOWN: " + err.Error()
		}
	} else {
		pgStatus = "NOT_CONFIGURED"
	}

	redisStatus := "UP"
	if s.rdb != nil {
		if err := s.rdb.Ping(ctx); err != nil {
			redisStatus = "DOWN: " + err.Error()
		}
	} else {
		redisStatus = "NOT_CONFIGURED"
	}

	isReady := (s.pgClient == nil || pgStatus == "UP") && (s.rdb == nil || redisStatus == "UP")

	statusCode := http.StatusOK
	if !isReady {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, gin.H{
		"status": map[string]string{
			"postgres": pgStatus,
			"redis":    redisStatus,
		},
		"ready":     isReady,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
