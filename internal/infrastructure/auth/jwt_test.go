package auth_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/domain/user"
	"github.com/forgeflow/forgeflow/internal/infrastructure/auth"
)

func TestJWTManager_GenerateAndValidate(t *testing.T) {
	manager := auth.NewJWTManager("test-super-secret-key-32-bytes-long!", 1*time.Hour)

	u := &user.User{
		ID:    uuid.New(),
		Email: "engineer@forgeflow.dev",
		Roles: []user.Role{user.RoleAdmin, user.RoleOperator},
	}

	tokenStr, err := manager.GenerateToken(u)
	require.NoError(t, err)
	assert.NotEmpty(t, tokenStr)

	claims, err := manager.ValidateToken(tokenStr)
	require.NoError(t, err)
	assert.Equal(t, u.ID, claims.UserID)
	assert.Equal(t, u.Email, claims.Email)
	assert.Equal(t, u.Roles, claims.Roles)
}

func TestJWTManager_InvalidTokens(t *testing.T) {
	manager := auth.NewJWTManager("test-super-secret-key-32-bytes-long!", 1*time.Hour)
	otherManager := auth.NewJWTManager("different-secret-key-that-will-fail!!", 1*time.Hour)

	u := &user.User{
		ID:    uuid.New(),
		Email: "test@forgeflow.dev",
		Roles: []user.Role{user.RoleUser},
	}

	tokenStr, _ := manager.GenerateToken(u)

	// Validate with different key -> must fail
	_, err := otherManager.ValidateToken(tokenStr)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)

	// Validate malformed token -> must fail
	_, err = manager.ValidateToken("invalid.token.string")
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
}
