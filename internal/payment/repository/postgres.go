package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/domain"
)

var (
	ErrPaymentNotFound     = errors.New("payment not found")
	ErrDuplicateIdempotKey = errors.New("idempotency key already used")
	// ErrActivePaymentExists — у заказа уже есть незавершённый или успешный платёж.
	// Именно этот случай раньше приводил к повторному списанию: до появления
	// частичного уникального индекса параллельные запросы создавали несколько платежей.
	ErrActivePaymentExists = errors.New("order already has an active payment")
)

const (
	uniqueViolation           = "23505"
	activePaymentIndex        = "payments_order_id_active_uq"
	idempotencyKeyIndexSuffix = "idempotency_key_uq"
)

type Repository interface {
	Create(ctx context.Context, payment domain.Payment, event kafka.Event, topic string) error
	GetByID(ctx context.Context, paymentID string) (domain.Payment, error)
	FindByIdempotencyKey(ctx context.Context, orderID, userID, key string) (domain.Payment, error)
	FindActiveByOrder(ctx context.Context, orderID, userID string) (domain.Payment, error)
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) Create(ctx context.Context, payment domain.Payment, event kafka.Event, topic string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin create payment: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		insert into payments (id, order_id, user_id, status, amount, idempotency_key, created_at, updated_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8)
	`, payment.ID, payment.OrderID, payment.UserID, payment.Status, payment.Amount.String(),
		nullableString(payment.IdempotencyKey), payment.CreatedAt, payment.UpdatedAt)
	if err != nil {
		if constraint, ok := uniqueViolationConstraint(err); ok {
			// Два разных ограничения требуют разной реакции: по ключу
			// идемпотентности возвращается прежний результат, по активному
			// платежу — отказ с 409.
			if constraint == activePaymentIndex {
				return ErrActivePaymentExists
			}
			if strings.HasSuffix(constraint, idempotencyKeyIndexSuffix) {
				return ErrDuplicateIdempotKey
			}
			return ErrDuplicateIdempotKey
		}
		return fmt.Errorf("create payment: %w", err)
	}

	// Платёж и событие о нём фиксируются одной транзакцией.
	if err := outbox.InsertTx(ctx, tx, topic, event); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create payment: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, paymentID string) (domain.Payment, error) {
	return r.findOne(ctx, `where id = $1`, paymentID)
}

func (r *PostgresRepository) FindByIdempotencyKey(ctx context.Context, orderID, userID, key string) (domain.Payment, error) {
	return r.findOne(ctx, `where order_id = $1 and user_id = $2 and idempotency_key = $3`, orderID, userID, key)
}

func (r *PostgresRepository) FindActiveByOrder(ctx context.Context, orderID, userID string) (domain.Payment, error) {
	return r.findOne(ctx,
		`where order_id = $1 and user_id = $2 and status in ('pending', 'succeeded')`,
		orderID, userID)
}

func (r *PostgresRepository) findOne(ctx context.Context, where string, args ...any) (domain.Payment, error) {
	var (
		payment domain.Payment
		amount  string
	)
	err := r.db.QueryRow(ctx, `
		select id, order_id, user_id, status, amount::text,
		       coalesce(idempotency_key, ''), created_at, updated_at
		from payments `+where, args...).Scan(
		&payment.ID, &payment.OrderID, &payment.UserID, &payment.Status,
		&amount, &payment.IdempotencyKey, &payment.CreatedAt, &payment.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Payment{}, ErrPaymentNotFound
		}
		return domain.Payment{}, fmt.Errorf("find payment: %w", err)
	}

	payment.Amount, err = money.Parse(amount)
	if err != nil {
		return domain.Payment{}, fmt.Errorf("parse payment amount %q: %w", amount, err)
	}
	return payment, nil
}

func uniqueViolationConstraint(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return pgErr.ConstraintName, true
	}
	return "", false
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
