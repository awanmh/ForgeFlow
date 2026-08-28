package user_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/domain/user"
)

func TestNewUser_Validation(t *testing.T) {
	t.Run("valid user creation and auth", func(t *testing.T) {
		u, err := user.NewUser("admin@forgeflow.dev", "SecurePassword123!", "Admin User", []user.Role{user.RoleAdmin})
		require.NoError(t, err)
		assert.Equal(t, "admin@forgeflow.dev", u.Email)
		assert.True(t, u.Authenticate("SecurePassword123!"))
		assert.False(t, u.Authenticate("WrongPassword"))
		assert.True(t, u.HasRole(user.RoleAdmin))
		assert.True(t, u.HasRole(user.RoleUser)) // Admin inherits user permissions
	})

	t.Run("invalid email", func(t *testing.T) {
		_, err := user.NewUser("invalid-email", "SecurePassword123!", "Name", nil)
		require.ErrorIs(t, err, user.ErrInvalidEmail)
	})

	t.Run("short password", func(t *testing.T) {
		_, err := user.NewUser("test@forgeflow.dev", "short", "Name", nil)
		require.ErrorIs(t, err, user.ErrInvalidPassword)
	})

	t.Run("inactive user cannot authenticate", func(t *testing.T) {
		u, err := user.NewUser("disabled@forgeflow.dev", "SecurePassword123!", "Disabled User", nil)
		require.NoError(t, err)
		u.Status = user.StatusDisabled
		assert.False(t, u.Authenticate("SecurePassword123!"))
	})

	t.Run("record login", func(t *testing.T) {
		u, err := user.NewUser("login@forgeflow.dev", "SecurePassword123!", "User", nil)
		require.NoError(t, err)
		assert.Nil(t, u.LastLoginAt)

		loginTime := time.Now().UTC()
		u.RecordLogin(loginTime)
		require.NotNil(t, u.LastLoginAt)
		assert.Equal(t, loginTime, *u.LastLoginAt)
	})
}
