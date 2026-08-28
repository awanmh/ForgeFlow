package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/forgeflow/forgeflow/internal/domain/user"
)

type UserRepo struct {
	client *Client
}

func NewUserRepo(client *Client) *UserRepo {
	return &UserRepo{client: client}
}

func (r *UserRepo) Create(ctx context.Context, u *user.User) error {
	return r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		query := `
			INSERT INTO users (
				id, email, password_hash, name, status, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7
			)
		`
		_, err := tx.Exec(ctx, query,
			u.ID, u.Email, u.PasswordHash, u.Name, string(u.Status), u.CreatedAt, u.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert user: %w", err)
		}

		for _, role := range u.Roles {
			roleQuery := `
				INSERT INTO user_roles (user_id, role_id)
				SELECT $1, id FROM roles WHERE name = $2
				ON CONFLICT DO NOTHING
			`
			_, err = tx.Exec(ctx, roleQuery, u.ID, string(role))
			if err != nil {
				return fmt.Errorf("failed to assign role %s: %w", role, err)
			}
		}

		return nil
	})
}

func (r *UserRepo) GetByID(ctx context.Context, id uuid.UUID) (*user.User, error) {
	query := `
		SELECT id, email, password_hash, name, status, created_at, updated_at, last_login_at
		FROM users
		WHERE id = $1
	`
	u := &user.User{}
	var statusStr string
	err := r.client.Pool.QueryRow(ctx, query, id).Scan(
		&u.ID, &u.Email, &u.PasswordHash, &u.Name, &statusStr, &u.CreatedAt, &u.UpdatedAt, &u.LastLoginAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, user.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to fetch user: %w", err)
	}
	u.Status = user.Status(statusStr)

	roles, err := r.getUserRoles(ctx, id)
	if err != nil {
		return nil, err
	}
	u.Roles = roles

	return u, nil
}

func (r *UserRepo) GetByEmail(ctx context.Context, email string) (*user.User, error) {
	query := `
		SELECT id, email, password_hash, name, status, created_at, updated_at, last_login_at
		FROM users
		WHERE email = $1
	`
	u := &user.User{}
	var statusStr string
	err := r.client.Pool.QueryRow(ctx, query, email).Scan(
		&u.ID, &u.Email, &u.PasswordHash, &u.Name, &statusStr, &u.CreatedAt, &u.UpdatedAt, &u.LastLoginAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, user.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to fetch user by email: %w", err)
	}
	u.Status = user.Status(statusStr)

	roles, err := r.getUserRoles(ctx, u.ID)
	if err != nil {
		return nil, err
	}
	u.Roles = roles

	return u, nil
}

func (r *UserRepo) Update(ctx context.Context, u *user.User) error {
	query := `
		UPDATE users
		SET name = $1, status = $2, updated_at = $3, last_login_at = $4
		WHERE id = $5
	`
	_, err := r.client.Pool.Exec(ctx, query, u.Name, string(u.Status), u.UpdatedAt, u.LastLoginAt, u.ID)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}
	return nil
}

func (r *UserRepo) AssignRole(ctx context.Context, userID uuid.UUID, role user.Role) error {
	query := `
		INSERT INTO user_roles (user_id, role_id)
		SELECT $1, id FROM roles WHERE name = $2
		ON CONFLICT DO NOTHING
	`
	_, err := r.client.Pool.Exec(ctx, query, userID, string(role))
	if err != nil {
		return fmt.Errorf("failed to assign role: %w", err)
	}
	return nil
}

func (r *UserRepo) getUserRoles(ctx context.Context, userID uuid.UUID) ([]user.Role, error) {
	query := `
		SELECT r.name
		FROM roles r
		JOIN user_roles ur ON ur.role_id = r.id
		WHERE ur.user_id = $1
	`
	rows, err := r.client.Pool.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to query user roles: %w", err)
	}
	defer rows.Close()

	var roles []user.Role
	for rows.Next() {
		var roleName string
		if err := rows.Scan(&roleName); err != nil {
			return nil, err
		}
		roles = append(roles, user.Role(roleName))
	}
	return roles, nil
}
