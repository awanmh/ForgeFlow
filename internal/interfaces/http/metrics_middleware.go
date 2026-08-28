package http

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/forgeflow/forgeflow/internal/infrastructure/metrics"
)

// PrometheusMetricsMiddleware records request duration and total counts for Prometheus.
func PrometheusMetricsMiddleware() gin.HandlerFunc {
	m := metrics.GetMetrics()

	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}

		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		method := c.Request.Method

		m.HTTPRequestsTotal.WithLabelValues(method, path, status).Inc()
		m.HTTPRequestDurationSecs.WithLabelValues(method, path).Observe(duration)
	}
}
