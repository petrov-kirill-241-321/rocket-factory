package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
)

var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrDuplicateEvent      = errors.New("event already processed")
	ErrDuplicateIdempotKey = errors.New("idempotency key already used")
)

const uniqueViolation = "23505"

// ApplyStatusParams описывает попытку применить статус из доменного события.
type ApplyStatusParams struct {
	OrderID      string
	Status       string
	EventID      string
	EventType    string
	ConsumerName string
}

// StatusUpdate — результат применения статуса.
// Applied=false означает, что событие устарело: заказ уже в более позднем
// состоянии. Это не ошибка, а нормальный исход при доставке at-least-once.
type StatusUpdate struct {
	Order          domain.Order
	PreviousStatus string
	Applied        bool
}

type Repository interface {
	Create(ctx context.Context, order domain.Order, event kafka.Event, topic string) error
	GetByID(ctx context.Context, orderID, userID string) (domain.Order, error)
	GetByIDAnyUser(ctx context.Context, orderID string) (domain.Order, error)
	ListByUser(ctx context.Context, userID string, limit, offset int) ([]domain.Order, error)
	FindByIdempotencyKey(ctx context.Context, userID, key string) (domain.Order, error)
	ApplyStatus(ctx context.Context, params ApplyStatusParams) (StatusUpdate, error)
}

// CatalogRepository — источник истины по ценам. Вынесен отдельно от заказов,
// потому что это разные агрегаты с разным временем жизни.
type CatalogRepository interface {
	FindActiveBySKUs(ctx context.Context, skus []string) (map[string]domain.CatalogItem, error)
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) Create(ctx context.Context, order domain.Order, event kafka.Event, topic string) error {
	return r.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			insert into orders (id, user_id, status, total_amount, idempotency_key, created_at, updated_at)
			values ($1, $2, $3, $4, $5, $6, $7)
		`, order.ID, order.UserID, order.Status, order.TotalAmount.String(),
			nullableString(order.IdempotencyKey), order.CreatedAt, order.UpdatedAt)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrDuplicateIdempotKey
			}
			return fmt.Errorf("insert order: %w", err)
		}

		// CopyFrom вместо цикла INSERT: одна операция вместо N round-trip'ов.
		rows := make([][]any, 0, len(order.Items))
		for _, item := range order.Items {
			rows = append(rows, []any{
				item.ID, order.ID, item.SKU, item.Name,
				item.Quantity, item.UnitPrice.String(), item.CreatedAt,
			})
		}
		_, err = tx.CopyFrom(ctx,
			pgx.Identifier{"order_items"},
			[]string{"id", "order_id", "sku", "name", "quantity", "unit_price", "created_at"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("insert order items: %w", err)
		}

		// Событие пишется в той же транзакции: заказ и факт его создания
		// либо появляются вместе, либо не появляются вовсе.
		return outbox.InsertTx(ctx, tx, topic, event)
	})
}

func (r *PostgresRepository) ApplyStatus(ctx context.Context, params ApplyStatusParams) (StatusUpdate, error) {
	var result StatusUpdate

	err := r.inTx(ctx, func(tx pgx.Tx) error {
		// Отметка об обработке события идёт в той же транзакции, что и изменение
		// состояния: повторная доставка не может применить статус дважды.
		_, err := tx.Exec(ctx, `
			insert into processed_events (event_id, event_type, consumer_name, processed_at)
			values ($1, $2, $3, now())
		`, params.EventID, params.EventType, params.ConsumerName)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrDuplicateEvent
			}
			return fmt.Errorf("insert processed event: %w", err)
		}

		var currentStatus string
		err = tx.QueryRow(ctx, `select status from orders where id = $1 for update`, params.OrderID).Scan(&currentStatus)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrOrderNotFound
			}
			return fmt.Errorf("lock order: %w", err)
		}
		result.PreviousStatus = currentStatus

		apply, err := domain.ShouldApply(currentStatus, params.Status)
		if err != nil {
			return err
		}
		if !apply {
			// Событие устарело. Отметку об обработке всё равно фиксируем,
			// иначе оно будет бесконечно возвращаться при перечитывании топика.
			result.Applied = false
			return nil
		}

		tag, err := tx.Exec(ctx, `
			update orders set status = $2, updated_at = now() where id = $1
		`, params.OrderID, params.Status)
		if err != nil {
			return fmt.Errorf("update order status: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrOrderNotFound
		}
		result.Applied = true
		return nil
	})
	if err != nil {
		return StatusUpdate{}, err
	}

	order, err := r.GetByIDAnyUser(ctx, params.OrderID)
	if err != nil {
		return StatusUpdate{}, err
	}
	result.Order = order
	return result, nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, orderID, userID string) (domain.Order, error) {
	return r.getOne(ctx, `where o.id = $1 and o.user_id = $2`, orderID, userID)
}

func (r *PostgresRepository) GetByIDAnyUser(ctx context.Context, orderID string) (domain.Order, error) {
	return r.getOne(ctx, `where o.id = $1`, orderID)
}

func (r *PostgresRepository) FindByIdempotencyKey(ctx context.Context, userID, key string) (domain.Order, error) {
	return r.getOne(ctx, `where o.user_id = $1 and o.idempotency_key = $2`, userID, key)
}

func (r *PostgresRepository) ListByUser(ctx context.Context, userID string, limit, offset int) ([]domain.Order, error) {
	// Один запрос вместо выборки заказов и последующей загрузки позиций по одной.
	rows, err := r.db.Query(ctx, `
		with selected_orders as (
			select id, user_id, status, total_amount, idempotency_key, created_at, updated_at
			from orders
			where user_id = $1
			order by created_at desc
			limit $2 offset $3
		)
		select
			o.id, o.user_id, o.status, o.total_amount::text,
			coalesce(o.idempotency_key, ''), o.created_at, o.updated_at,
			oi.id, oi.order_id, oi.sku, oi.name, oi.quantity, oi.unit_price::text, oi.created_at
		from selected_orders o
		left join order_items oi on oi.order_id = o.id
		order by o.created_at desc, oi.sku asc
	`, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	orders := make([]domain.Order, 0)
	index := make(map[string]int)

	for rows.Next() {
		var (
			order         domain.Order
			totalAmount   string
			itemID        sql.NullString
			itemOrderID   sql.NullString
			sku           sql.NullString
			name          sql.NullString
			unitPrice     sql.NullString
			quantity      sql.NullInt64
			itemCreatedAt sql.NullTime
		)
		if err := rows.Scan(
			&order.ID, &order.UserID, &order.Status, &totalAmount,
			&order.IdempotencyKey, &order.CreatedAt, &order.UpdatedAt,
			&itemID, &itemOrderID, &sku, &name, &quantity, &unitPrice, &itemCreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}

		position, exists := index[order.ID]
		if !exists {
			order.TotalAmount, err = money.Parse(totalAmount)
			if err != nil {
				return nil, fmt.Errorf("parse order total %q: %w", totalAmount, err)
			}
			order.Items = make([]domain.Item, 0)
			orders = append(orders, order)
			position = len(orders) - 1
			index[order.ID] = position
		}

		if itemID.Valid {
			price, err := money.Parse(unitPrice.String)
			if err != nil {
				return nil, fmt.Errorf("parse item price %q: %w", unitPrice.String, err)
			}
			orders[position].Items = append(orders[position].Items, domain.Item{
				ID:        itemID.String,
				OrderID:   itemOrderID.String,
				SKU:       sku.String,
				Name:      name.String,
				Quantity:  int(quantity.Int64),
				UnitPrice: price,
				CreatedAt: itemCreatedAt.Time,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate orders: %w", err)
	}
	return orders, nil
}

func (r *PostgresRepository) getOne(ctx context.Context, where string, args ...any) (domain.Order, error) {
	var (
		order       domain.Order
		totalAmount string
	)
	err := r.db.QueryRow(ctx, `
		select o.id, o.user_id, o.status, o.total_amount::text,
		       coalesce(o.idempotency_key, ''), o.created_at, o.updated_at
		from orders o `+where, args...).Scan(
		&order.ID, &order.UserID, &order.Status, &totalAmount,
		&order.IdempotencyKey, &order.CreatedAt, &order.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Order{}, ErrOrderNotFound
		}
		return domain.Order{}, fmt.Errorf("get order: %w", err)
	}

	order.TotalAmount, err = money.Parse(totalAmount)
	if err != nil {
		return domain.Order{}, fmt.Errorf("parse order total %q: %w", totalAmount, err)
	}

	items, err := r.listItems(ctx, order.ID)
	if err != nil {
		return domain.Order{}, err
	}
	order.Items = items
	return order, nil
}

func (r *PostgresRepository) listItems(ctx context.Context, orderID string) ([]domain.Item, error) {
	rows, err := r.db.Query(ctx, `
		select id, order_id, sku, name, quantity, unit_price::text, created_at
		from order_items
		where order_id = $1
		order by sku
	`, orderID)
	if err != nil {
		return nil, fmt.Errorf("list order items: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Item, 0)
	for rows.Next() {
		var (
			item      domain.Item
			unitPrice string
		)
		if err := rows.Scan(&item.ID, &item.OrderID, &item.SKU, &item.Name,
			&item.Quantity, &unitPrice, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan order item: %w", err)
		}
		item.UnitPrice, err = money.Parse(unitPrice)
		if err != nil {
			return nil, fmt.Errorf("parse item price %q: %w", unitPrice, err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order items: %w", err)
	}
	return items, nil
}

// inTx выполняет функцию в транзакции, гарантируя откат при ошибке.
func (r *PostgresRepository) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
