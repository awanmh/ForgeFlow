package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/domain/user"
	infraAuth "github.com/forgeflow/forgeflow/internal/infrastructure/auth"
)

const (
	ContextUserID    = "user_id"
	ContextUserEmail = "user_email"
	ContextUserRoles = "user_roles"
)

// AuthMiddleware extracts and validates Bearer JWT tokens from the Authorization header.
func AuthMiddleware(jwtManager *infraAuth.JWTManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":       "UNAUTHORIZED",
					"message":    "Missing Authorization header",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":       "INVALID_AUTH_HEADER",
					"message":    "Authorization header must follow 'Bearer <token>' format",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		claims, err := jwtManager.ValidateToken(parts[1])
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":       "INVALID_TOKEN",
					"message":    "Token is invalid or expired",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.Set(ContextUserID, claims.UserID)
		c.Set(ContextUserEmail, claims.Email)
		c.Set(ContextUserRoles, claims.Roles)

		c.Next()
	}
}

// OptionalAuthMiddleware extracts claims if Bearer token is provided, but does not abort if missing.
func OptionalAuthMiddleware(jwtManager *infraAuth.JWTManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader != "" {
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
				if claims, err := jwtManager.ValidateToken(parts[1]); err == nil {
					c.Set(ContextUserID, claims.UserID)
					c.Set(ContextUserEmail, claims.Email)
					c.Set(ContextUserRoles, claims.Roles)
				}
			}
		}
		c.Next()
	}
}

// RequireRoles enforces RBAC authorization check. Admin role always passes.
func RequireRoles(requiredRoles ...user.Role) gin.HandlerFunc {
	return func(c *gin.Context) {
		rolesVal, exists := c.Get(ContextUserRoles)
		if !exists {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":       "FORBIDDEN",
					"message":    "User does not have required permissions",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		userRoles, ok := rolesVal.([]user.Role)
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":       "FORBIDDEN",
					"message":    "Failed to evaluate user roles",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		// Check if user has admin or any of the required roles
		hasPermission := false
		for _, ur := range userRoles {
			if ur == user.RoleAdmin {
				hasPermission = true
				break
			}
			for _, req := range requiredRoles {
				if ur == req {
					hasPermission = true
					break
				}
			}
			if hasPermission {
				break
			}
		}

		if !hasPermission {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":       "FORBIDDEN",
					"message":    "Insufficient permissions for this operation",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.Next()
	}
}

// GetContextUserID helper retrieves the user UUID from the Gin context.
func GetContextUserID(c *gin.Context) uuid.UUID {
	if val, exists := c.Get(ContextUserID); exists {
		if uid, ok := val.(uuid.UUID); ok {
			return uid
		}
	}
	return uuid.Nil
}
