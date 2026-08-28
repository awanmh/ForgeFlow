package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/forgeflow/forgeflow/internal/domain/user"
)

var (
	ErrInvalidToken = errors.New("invalid or expired authentication token")
	ErrMissingClaim = errors.New("token missing required claims")
)

type CustomClaims struct {
	UserID uuid.UUID   `json:"user_id"`
	Email  string      `json:"email"`
	Roles  []user.Role `json:"roles"`
	jwt.RegisteredClaims
}

type JWTManager struct {
	secretKey     []byte
	tokenDuration time.Duration
	issuer        string
}

func NewJWTManager(secretKey string, tokenDuration time.Duration) *JWTManager {
	if secretKey == "" {
		secretKey = "forgeflow-default-development-jwt-secret-key-32b"
	}
	if tokenDuration <= 0 {
		tokenDuration = 24 * time.Hour
	}
	return &JWTManager{
		secretKey:     []byte(secretKey),
		tokenDuration: tokenDuration,
		issuer:        "forgeflow",
	}
}

// GenerateToken creates a signed JWT for an authenticated user.
func (m *JWTManager) GenerateToken(u *user.User) (string, error) {
	now := time.Now().UTC()
	claims := CustomClaims{
		UserID: u.ID,
		Email:  u.Email,
		Roles:  u.Roles,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(m.tokenDuration)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    m.issuer,
			Subject:   u.ID.String(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(m.secretKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign jwt token: %w", err)
	}

	return signed, nil
}

// ValidateToken parses and validates a signed JWT string.
func (m *JWTManager) ValidateToken(tokenStr string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &CustomClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.secretKey, nil
	})

	if err != nil {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	if claims.UserID == uuid.Nil {
		return nil, ErrMissingClaim
	}

	return claims, nil
}
