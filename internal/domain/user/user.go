package user

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrUserNotFound     = errors.New("user not found")
	ErrInvalidEmail     = errors.New("invalid email address")
	ErrInvalidPassword  = errors.New("password does not meet security requirements")
	ErrUserInactive     = errors.New("user account is not active")
	ErrDuplicateEmail   = errors.New("email already exists")
	ErrInvalidRole      = errors.New("invalid user role")
)

type Status string

const (
	StatusActive   Status = "ACTIVE"
	StatusDisabled Status = "DISABLED"
	StatusLocked   Status = "LOCKED"
)

type Role string

const (
	RoleAdmin    Role = "ADMIN"
	RoleOperator Role = "OPERATOR"
	RoleUser     Role = "USER"
	RoleViewer   Role = "VIEWER"
)

type User struct {
	ID           uuid.UUID  `json:"id"`
	Email        string     `json:"email"`
	PasswordHash string     `json:"-"`
	Name         string     `json:"name"`
	Status       Status     `json:"status"`
	Roles        []Role     `json:"roles"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	LastLoginAt  *time.Time `json:"last_login_at,omitempty"`
}

func NewUser(email, plainPassword, name string, roles []Role) (*User, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	if email == "" || !strings.Contains(email, "@") {
		return nil, ErrInvalidEmail
	}
	if len(plainPassword) < 8 {
		return nil, fmt.Errorf("%w: minimum 8 characters required", ErrInvalidPassword)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = email
	}
	if len(roles) == 0 {
		roles = []Role{RoleUser}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(plainPassword), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	now := time.Now().UTC()
	return &User{
		ID:           uuid.New(),
		Email:        email,
		PasswordHash: string(hash),
		Name:         name,
		Status:       StatusActive,
		Roles:        roles,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (u *User) Authenticate(plainPassword string) bool {
	if u.Status != StatusActive {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(plainPassword))
	return err == nil
}

func (u *User) HasRole(role Role) bool {
	for _, r := range u.Roles {
		if r == role || r == RoleAdmin {
			return true
		}
	}
	return false
}

func (u *User) RecordLogin(now time.Time) {
	nowUTC := now.UTC()
	u.LastLoginAt = &nowUTC
	u.UpdatedAt = nowUTC
}
