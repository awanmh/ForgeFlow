package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/domain/user"
	infraAuth "github.com/forgeflow/forgeflow/internal/infrastructure/auth"
	"github.com/forgeflow/forgeflow/internal/ports"
)

var (
	ErrInvalidCredentials = errors.New("invalid email or password")
)

type RegisterCommand struct {
	Email    string      `json:"email"`
	Password string      `json:"password"`
	Name     string      `json:"name"`
	Roles    []user.Role `json:"roles"`
}

type LoginCommand struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthResult struct {
	Token string     `json:"token"`
	User  *user.User `json:"user"`
}

type Service struct {
	userRepo   ports.UserRepository
	jwtManager *infraAuth.JWTManager
}

func NewService(userRepo ports.UserRepository, jwtManager *infraAuth.JWTManager) *Service {
	return &Service{
		userRepo:   userRepo,
		jwtManager: jwtManager,
	}
}

func (s *Service) Register(ctx context.Context, cmd RegisterCommand) (*AuthResult, error) {
	// Check if user already exists
	existing, err := s.userRepo.GetByEmail(ctx, cmd.Email)
	if err == nil && existing != nil {
		return nil, user.ErrDuplicateEmail
	}

	u, err := user.NewUser(cmd.Email, cmd.Password, cmd.Name, cmd.Roles)
	if err != nil {
		return nil, err
	}

	if err := s.userRepo.Create(ctx, u); err != nil {
		return nil, fmt.Errorf("failed to persist user: %w", err)
	}

	token, err := s.jwtManager.GenerateToken(u)
	if err != nil {
		return nil, err
	}

	return &AuthResult{
		Token: token,
		User:  u,
	}, nil
}

func (s *Service) Login(ctx context.Context, cmd LoginCommand) (*AuthResult, error) {
	u, err := s.userRepo.GetByEmail(ctx, cmd.Email)
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	if !u.Authenticate(cmd.Password) {
		return nil, ErrInvalidCredentials
	}

	u.RecordLogin(time.Now().UTC())
	_ = s.userRepo.Update(ctx, u)

	token, err := s.jwtManager.GenerateToken(u)
	if err != nil {
		return nil, fmt.Errorf("failed to generate auth token: %w", err)
	}

	return &AuthResult{
		Token: token,
		User:  u,
	}, nil
}

func (s *Service) GetProfile(ctx context.Context, userID uuid.UUID) (*user.User, error) {
	return s.userRepo.GetByID(ctx, userID)
}
