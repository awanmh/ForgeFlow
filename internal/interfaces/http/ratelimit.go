package http

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/forgeflow/forgeflow/internal/infrastructure/redis"
)

// RateLimiter provides rate limiting middleware using Redis.
type RateLimiter struct {
	rdb        *redis.Client
	limit      int64
	windowSecs int64
}

// NewRateLimiter constructs a new rate limiter.
func NewRateLimiter(rdb *redis.Client, limit int64, window time.Duration) *RateLimiter {
	if limit <= 0 {
		limit = 100 // 100 requests
	}
	windowSecs := int64(window.Seconds())
	if windowSecs <= 0 {
		windowSecs = 60 // per 60 seconds
	}
	return &RateLimiter{
		rdb:        rdb,
		limit:      limit,
		windowSecs: windowSecs,
	}
}

// Middleware creates a Gin middleware handler enforcing rate limits.
func (rl *RateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if rl.rdb == nil {
			c.Next()
			return
		}

		clientKey := c.ClientIP()
		if uid := GetContextUserID(c); uid.String() != "00000000-0000-0000-0000-000000000000" {
			clientKey = uid.String()
		}

		redisKey := fmt.Sprintf("forgeflow:ratelimit:%s", clientKey)
		ctx := c.Request.Context()

		count, err := rl.rdb.UniversalClient.Incr(ctx, redisKey).Result()
		if err != nil {
			// Fail open on Redis error so legitimate traffic is not blocked
			c.Next()
			return
		}

		if count == 1 {
			_ = rl.rdb.UniversalClient.Expire(ctx, redisKey, time.Duration(rl.windowSecs)*time.Second)
		}

		remaining := rl.limit - count
		if remaining < 0 {
			remaining = 0
		}

		c.Header("X-RateLimit-Limit", strconv.FormatInt(rl.limit, 10))
		c.Header("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(time.Now().Unix()+rl.windowSecs, 10))

		if count > rl.limit {
			c.Header("Retry-After", strconv.FormatInt(rl.windowSecs, 10))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": gin.H{
					"code":       "RATE_LIMIT_EXCEEDED",
					"message":    fmt.Sprintf("Rate limit of %d requests per %d seconds exceeded", rl.limit, rl.windowSecs),
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.Next()
	}
}
