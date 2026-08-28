package http

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_Health(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	router := NewRouter(nil, nil, logger)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/health", nil)
	router.Engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "UP", resp["status"])
	assert.NotEmpty(t, w.Header().Get("X-Request-ID"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", w.Header().Get("X-Frame-Options"))
}

func TestRouter_CustomRequestID(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	router := NewRouter(nil, nil, logger)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/health", nil)
	req.Header.Set("X-Request-ID", "custom-trace-id-999")
	router.Engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "custom-trace-id-999", w.Header().Get("X-Request-ID"))
}

func TestRouter_PanicRecovery(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	router := NewRouter(nil, nil, logger)

	// Add panic route for testing
	router.Engine.GET("/test/panic", func(c *gin.Context) {
		panic("deliberate test panic")
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test/panic", nil)
	router.Engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var resp map[string]any
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	errObj, ok := resp["error"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INTERNAL_SERVER_ERROR", errObj["code"])
}
