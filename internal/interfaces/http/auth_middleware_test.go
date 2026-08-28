package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/domain/user"
	infraAuth "github.com/forgeflow/forgeflow/internal/infrastructure/auth"
	httpInterface "github.com/forgeflow/forgeflow/internal/interfaces/http"
)

func TestAuthMiddleware_ValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := infraAuth.NewJWTManager("test-secret-key-32-bytes-long-for-testing", 1*time.Hour)

	r := gin.New()
	r.Use(httpInterface.AuthMiddleware(jwtManager))
	r.GET("/protected", func(c *gin.Context) {
		userID := httpInterface.GetContextUserID(c)
		c.JSON(http.StatusOK, gin.H{"user_id": userID.String()})
	})

	u := &user.User{ID: uuid.New(), Email: "test@forgeflow.dev", Roles: []user.Role{user.RoleUser}}
	token, err := jwtManager.GenerateToken(u)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_MissingOrInvalidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := infraAuth.NewJWTManager("test-secret-key-32-bytes-long-for-testing", 1*time.Hour)

	r := gin.New()
	r.Use(httpInterface.AuthMiddleware(jwtManager))
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	t.Run("missing header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("invalid token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", "Bearer invalid-jwt-token")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestRequireRoles(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := infraAuth.NewJWTManager("test-secret-key-32-bytes-long-for-testing", 1*time.Hour)

	r := gin.New()
	r.Use(httpInterface.AuthMiddleware(jwtManager))
	r.POST("/admin-only", httpInterface.RequireRoles(user.RoleAdmin), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "admin_granted"})
	})

	t.Run("admin user allowed", func(t *testing.T) {
		adminUser := &user.User{ID: uuid.New(), Email: "admin@forgeflow.dev", Roles: []user.Role{user.RoleAdmin}}
		token, _ := jwtManager.GenerateToken(adminUser)

		req := httptest.NewRequest(http.MethodPost, "/admin-only", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("standard user forbidden", func(t *testing.T) {
		stdUser := &user.User{ID: uuid.New(), Email: "user@forgeflow.dev", Roles: []user.Role{user.RoleUser}}
		token, _ := jwtManager.GenerateToken(stdUser)

		req := httptest.NewRequest(http.MethodPost, "/admin-only", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})
}
