package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/domain"
)

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrEmailAlreadyTaken = errors.New("email already taken")
)

type UserRepository interface {
	Create(ctx context.Context, user domain.User) error
	FindByEmail(ctx context.Context, email string) (domain.User, error)
	FindByID(ctx context.Context, id string) (domain.User, error)
}

type PostgresUserRepository struct {
	db *pgxpool.Pool
}

func NewPostgresUserRepository(db *pgxpool.Pool) *PostgresUserRepository {
	return &PostgresUserRepository{db: db}
}

func (r *PostgresUserRepository) Create(ctx context.Context, user domain.User) error {
	_, err := r.db.Exec(ctx, `
		insert into users (id, email, password_hash, created_at, updated_at)
		values ($1, $2, $3, $4, $5)
	`, user.ID, user.Email, user.PasswordHash, user.CreatedAt, user.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrEmailAlreadyTaken
		}
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func (r *PostgresUserRepository) FindByEmail(ctx context.Context, email string) (domain.User, error) {
	return r.findOne(ctx, `select id, email, password_hash, created_at, updated_at from users where email = $1`, email)
}

func (r *PostgresUserRepository) FindByID(ctx context.Context, id string) (domain.User, error) {
	return r.findOne(ctx, `select id, email, password_hash, created_at, updated_at from users where id = $1`, id)
}

func (r *PostgresUserRepository) findOne(ctx context.Context, query string, arg string) (domain.User, error) {
	var user domain.User
	err := r.db.QueryRow(ctx, query, arg).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, ErrUserNotFound
		}
		return domain.User{}, fmt.Errorf("find user: %w", err)
	}

	user.CreatedAt = user.CreatedAt.UTC().Truncate(time.Microsecond)
	user.UpdatedAt = user.UpdatedAt.UTC().Truncate(time.Microsecond)
	return user, nil
}
