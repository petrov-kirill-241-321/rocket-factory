package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
)

var (
	ErrReservationExists   = errors.New("reservation already exists")
	ErrReservationNotFound = errors.New("reservation not found")
	ErrDuplicateEvent      = errors.New("event already processed")
)

const uniqueViolation = "23505"

// EventContext описывает событие, вызвавшее операцию. Пустой EventID означает
// вызов не из консьюмера — тогда запись в processed_events не делается.
type EventContext struct {
	EventID      string
	EventType    string
	ConsumerName string
}

type ReserveParams struct {
	Reservation domain.Reservation
	Event       EventContext
	OutboxEvent kafka.Event
	OutboxTopic string
}

// SettleParams описывает завершение резерва: снятие удержания или списание.
type SettleParams struct {
	OrderID     string
	Reason      string
	Event       EventContext
	OutboxEvent kafka.Event
	OutboxTopic string
}

type Repository interface {
	CheckAvailability(ctx context.Context, items []domain.ReservationItem) ([]domain.Availability, error)
	Reserve(ctx context.Context, params ReserveParams) (domain.Reservation, error)
	Release(ctx context.Context, params SettleParams) (domain.Reservation, error)
	Commit(ctx context.Context, params SettleParams) (domain.Reservation, error)
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

// CheckAvailability выбирает все запрошенные позиции одним запросом.
// Раньше на каждый SKU шёл отдельный round-trip.
func (r *PostgresRepository) CheckAvailability(ctx context.Context, items []domain.ReservationItem) ([]domain.Availability, error) {
	skus := skusOf(items)

	rows, err := r.db.Query(ctx, `
		select sku, quantity_available - quantity_reserved
		from inventory_items
		where sku = any($1)
	`, skus)
	if err != nil {
		return nil, fmt.Errorf("check inventory availability: %w", err)
	}
	defer rows.Close()

	available := make(map[string]int, len(items))
	for rows.Next() {
		var (
			sku   string
			count int
		)
		if err := rows.Scan(&sku, &count); err != nil {
			return nil, fmt.Errorf("scan inventory availability: %w", err)
		}
		available[sku] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inventory availability: %w", err)
	}

	result := make([]domain.Availability, 0, len(items))
	for _, item := range items {
		count := available[item.SKU]
		result = append(result, domain.Availability{
			SKU:       item.SKU,
			Requested: item.Quantity,
			Available: count,
			Enough:    count >= item.Quantity,
		})
	}
	return result, nil
}

// Reserve удерживает остатки под заказ.
//
// Отметка о событии, изменение остатков, резерв, его позиции и исходящее
// событие пишутся в одной транзакции: частичного результата быть не может.
func (r *PostgresRepository) Reserve(ctx context.Context, params ReserveParams) (domain.Reservation, error) {
	reservation := params.Reservation

	err := r.inTx(ctx, func(tx pgx.Tx) error {
		if err := markEventProcessed(ctx, tx, params.Event); err != nil {
			return err
		}

		// Позиции уже отсортированы по SKU, и запрос сохраняет этот порядок:
		// это исключает взаимные блокировки параллельных резервов.
		locked, err := lockItems(ctx, tx, skusOf(reservation.Items))
		if err != nil {
			return err
		}

		for _, item := range reservation.Items {
			free, ok := locked[item.SKU]
			if !ok {
				reservation.Status = domain.ReservationStatusFailed
				reservation.Reason = "unknown sku " + item.SKU
				break
			}
			if free < item.Quantity {
				reservation.Status = domain.ReservationStatusFailed
				reservation.Reason = fmt.Sprintf("not enough inventory for sku %s: requested %d, available %d",
					item.SKU, item.Quantity, free)
				break
			}
		}

		if reservation.Status != domain.ReservationStatusFailed {
			reservation.Status = domain.ReservationStatusReserved
			if err := adjustQuantities(ctx, tx, reservation.Items, adjustReserve); err != nil {
				return err
			}
		}

		if err := insertReservation(ctx, tx, reservation); err != nil {
			return err
		}

		// Тип события определяется исходом: успех или отказ.
		event := params.OutboxEvent
		event.Payload["reservation_id"] = reservation.ID
		event.Payload["status"] = reservation.Status
		event.Payload["items"] = itemsPayload(reservation.Items)
		if reservation.Status == domain.ReservationStatusFailed {
			event.EventType = kafka.EventInventoryFailed
			event.Payload["reason"] = reservation.Reason
		}

		return outbox.InsertTx(ctx, tx, params.OutboxTopic, event)
	})
	if err != nil {
		return domain.Reservation{}, err
	}
	return reservation, nil
}

// Release снимает удержание: оплата не прошла, остатки возвращаются в продажу.
func (r *PostgresRepository) Release(ctx context.Context, params SettleParams) (domain.Reservation, error) {
	return r.settle(ctx, params, domain.ReservationStatusReleased, adjustRelease, kafka.EventInventoryReleased)
}

// Commit превращает удержание в списание: производство завершено.
func (r *PostgresRepository) Commit(ctx context.Context, params SettleParams) (domain.Reservation, error) {
	return r.settle(ctx, params, domain.ReservationStatusCommitted, adjustCommit, kafka.EventInventoryCommitted)
}

func (r *PostgresRepository) settle(
	ctx context.Context,
	params SettleParams,
	targetStatus string,
	adjust adjustment,
	eventType string,
) (domain.Reservation, error) {
	var reservation domain.Reservation

	err := r.inTx(ctx, func(tx pgx.Tx) error {
		if err := markEventProcessed(ctx, tx, params.Event); err != nil {
			return err
		}

		var (
			current string
			id      string
			userID  string
		)
		err := tx.QueryRow(ctx, `
			select id, user_id, status
			from reservations
			where order_id = $1
			for update
		`, params.OrderID).Scan(&id, &userID, &current)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrReservationNotFound
			}
			return fmt.Errorf("lock reservation: %w", err)
		}

		reservation = domain.Reservation{ID: id, OrderID: params.OrderID, UserID: userID, Status: current}

		// Завершить можно только активное удержание. Повторная доставка события
		// или отказавший резерв не должны менять остатки.
		if current != domain.ReservationStatusReserved {
			return nil
		}

		items, err := loadReservationItems(ctx, tx, id)
		if err != nil {
			return err
		}
		reservation.Items = items

		if err := adjustQuantities(ctx, tx, items, adjust); err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			update reservations
			set status = $2, reason = coalesce($3, reason), updated_at = now()
			where id = $1
		`, id, targetStatus, nullableString(params.Reason))
		if err != nil {
			return fmt.Errorf("update reservation status: %w", err)
		}
		reservation.Status = targetStatus

		event := params.OutboxEvent
		event.EventType = eventType
		event.UserID = userID
		event.Payload["reservation_id"] = id
		event.Payload["status"] = targetStatus
		event.Payload["items"] = itemsPayload(items)
		if params.Reason != "" {
			event.Payload["reason"] = params.Reason
		}

		return outbox.InsertTx(ctx, tx, params.OutboxTopic, event)
	})
	if err != nil {
		return domain.Reservation{}, err
	}
	return reservation, nil
}

// adjustment описывает, как операция меняет остатки.
type adjustment struct {
	availableDelta int
	reservedDelta  int
}

var (
	adjustReserve = adjustment{availableDelta: 0, reservedDelta: 1}
	adjustRelease = adjustment{availableDelta: 0, reservedDelta: -1}
	// Списание уменьшает и удержание, и физический остаток.
	adjustCommit = adjustment{availableDelta: -1, reservedDelta: -1}
)

func adjustQuantities(ctx context.Context, tx pgx.Tx, items []domain.ReservationItem, adjust adjustment) error {
	if len(items) == 0 {
		return nil
	}

	skus := make([]string, 0, len(items))
	quantities := make([]int32, 0, len(items))
	for _, item := range items {
		skus = append(skus, item.SKU)
		quantities = append(quantities, int32(item.Quantity))
	}

	// Один UPDATE вместо цикла. CHECK-ограничения таблицы не дадут остаткам
	// уйти в минус, даже если логика выше окажется неверной.
	tag, err := tx.Exec(ctx, `
		update inventory_items i
		set quantity_available = i.quantity_available + v.quantity * $3,
		    quantity_reserved  = i.quantity_reserved  + v.quantity * $4,
		    updated_at = now()
		from (select unnest($1::text[]) as sku, unnest($2::int[]) as quantity) v
		where i.sku = v.sku
	`, skus, quantities, adjust.availableDelta, adjust.reservedDelta)
	if err != nil {
		return fmt.Errorf("adjust inventory quantities: %w", err)
	}
	if int(tag.RowsAffected()) != len(items) {
		return fmt.Errorf("adjust inventory quantities: updated %d rows, expected %d",
			tag.RowsAffected(), len(items))
	}
	return nil
}

func lockItems(ctx context.Context, tx pgx.Tx, skus []string) (map[string]int, error) {
	rows, err := tx.Query(ctx, `
		select sku, quantity_available - quantity_reserved
		from inventory_items
		where sku = any($1)
		order by sku
		for update
	`, skus)
	if err != nil {
		return nil, fmt.Errorf("lock inventory items: %w", err)
	}
	defer rows.Close()

	locked := make(map[string]int, len(skus))
	for rows.Next() {
		var (
			sku  string
			free int
		)
		if err := rows.Scan(&sku, &free); err != nil {
			return nil, fmt.Errorf("scan locked inventory item: %w", err)
		}
		locked[sku] = free
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate locked inventory items: %w", err)
	}
	return locked, nil
}

func insertReservation(ctx context.Context, tx pgx.Tx, reservation domain.Reservation) error {
	_, err := tx.Exec(ctx, `
		insert into reservations (id, order_id, user_id, status, reason, created_at, updated_at)
		values ($1, $2, $3, $4, $5, $6, $7)
	`, reservation.ID, reservation.OrderID, reservation.UserID, reservation.Status,
		nullableString(reservation.Reason), reservation.CreatedAt, reservation.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrReservationExists
		}
		return fmt.Errorf("insert reservation: %w", err)
	}

	rows := make([][]any, 0, len(reservation.Items))
	for _, item := range reservation.Items {
		rows = append(rows, []any{
			uuid.NewString(), reservation.ID, item.SKU, item.Name, item.Quantity, reservation.CreatedAt,
		})
	}
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"reservation_items"},
		[]string{"id", "reservation_id", "sku", "name", "quantity", "created_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("insert reservation items: %w", err)
	}
	return nil
}

func loadReservationItems(ctx context.Context, tx pgx.Tx, reservationID string) ([]domain.ReservationItem, error) {
	rows, err := tx.Query(ctx, `
		select sku, name, quantity
		from reservation_items
		where reservation_id = $1
		order by sku
	`, reservationID)
	if err != nil {
		return nil, fmt.Errorf("load reservation items: %w", err)
	}
	defer rows.Close()

	items := make([]domain.ReservationItem, 0)
	for rows.Next() {
		var item domain.ReservationItem
		if err := rows.Scan(&item.SKU, &item.Name, &item.Quantity); err != nil {
			return nil, fmt.Errorf("scan reservation item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reservation items: %w", err)
	}
	return items, nil
}

func markEventProcessed(ctx context.Context, tx pgx.Tx, event EventContext) error {
	if event.EventID == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `
		insert into processed_events (event_id, event_type, consumer_name, processed_at)
		values ($1, $2, $3, now())
	`, event.EventID, event.EventType, event.ConsumerName)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrDuplicateEvent
		}
		return fmt.Errorf("insert processed event: %w", err)
	}
	return nil
}

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

func skusOf(items []domain.ReservationItem) []string {
	skus := make([]string, 0, len(items))
	for _, item := range items {
		skus = append(skus, item.SKU)
	}
	return skus
}

func itemsPayload(items []domain.ReservationItem) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"sku":      item.SKU,
			"name":     item.Name,
			"quantity": item.Quantity,
		})
	}
	return out
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
