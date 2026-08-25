//go:build integration

// Интеграционные тесты репозитория заказов.
//
// Запуск:
//
//	TEST_DATABASE_URL="postgres://rocket:rocket@localhost:5432/rocket_factory?sslmode=disable" \
//	  go test -tags=integration ./internal/order/repository -v
package repository

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL не задан")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newTestOrder(t *testing.T, userID string) domain.Order {
	t.Helper()

	price, err := money.Parse("10.00")
	if err != nil {
		t.Fatalf("parse price: %v", err)
	}

	now := time.Now().UTC()
	order := domain.Order{
		ID:             uuid.NewString(),
		UserID:         userID,
		Status:         domain.StatusCreated,
		TotalAmount:    price,
		IdempotencyKey: "integration-" + uuid.NewString(),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	order.Items = []domain.Item{{
		ID:        uuid.NewString(),
		OrderID:   order.ID,
		SKU:       "ENGINE-X1",
		Name:      "Engine X1",
		Quantity:  1,
		UnitPrice: price,
		CreatedAt: now,
	}}
	return order
}

func TestCreateAndReadOrder(t *testing.T) {
	pool := newTestPool(t)
	repo := NewPostgresRepository(pool)
	ctx := context.Background()

	userID := uuid.NewString()
	order := newTestOrder(t, userID)

	event := kafka.NewEvent(kafka.EventOrderCreated, order.ID, userID, map[string]any{})
	if err := repo.Create(ctx, order, event, "order.events"); err != nil {
		t.Fatalf("create order: %v", err)
	}

	got, err := repo.GetByID(ctx, order.ID, userID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if got.TotalAmount.String() != "10.00" {
		t.Fatalf("сумма = %s, ожидалось 10.00", got.TotalAmount)
	}
	if len(got.Items) != 1 || got.Items[0].SKU != "ENGINE-X1" {
		t.Fatalf("позиции прочитаны неверно: %+v", got.Items)
	}

	// Чужой пользователь не должен видеть заказ.
	if _, err := repo.GetByID(ctx, order.ID, uuid.NewString()); !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("err = %v, ожидалось ErrOrderNotFound", err)
	}
}

func TestDuplicateIdempotencyKeyIsRejected(t *testing.T) {
	pool := newTestPool(t)
	repo := NewPostgresRepository(pool)
	ctx := context.Background()

	userID := uuid.NewString()
	first := newTestOrder(t, userID)

	if err := repo.Create(ctx, first, kafka.NewEvent(kafka.EventOrderCreated, first.ID, userID, nil), "order.events"); err != nil {
		t.Fatalf("первый заказ: %v", err)
	}

	second := newTestOrder(t, userID)
	second.IdempotencyKey = first.IdempotencyKey

	err := repo.Create(ctx, second, kafka.NewEvent(kafka.EventOrderCreated, second.ID, userID, nil), "order.events")
	if !errors.Is(err, ErrDuplicateIdempotKey) {
		t.Fatalf("err = %v, ожидалось ErrDuplicateIdempotKey", err)
	}
}

// Повторная доставка события не должна применять статус дважды.
func TestApplyStatusRejectsDuplicateEvent(t *testing.T) {
	pool := newTestPool(t)
	repo := NewPostgresRepository(pool)
	ctx := context.Background()

	userID := uuid.NewString()
	order := newTestOrder(t, userID)
	if err := repo.Create(ctx, order, kafka.NewEvent(kafka.EventOrderCreated, order.ID, userID, nil), "order.events"); err != nil {
		t.Fatalf("create order: %v", err)
	}

	params := ApplyStatusParams{
		OrderID:      order.ID,
		Status:       domain.StatusInventoryReserved,
		EventID:      uuid.NewString(),
		EventType:    kafka.EventInventoryReserved,
		ConsumerName: "order-service-test",
	}

	update, err := repo.ApplyStatus(ctx, params)
	if err != nil {
		t.Fatalf("первое применение: %v", err)
	}
	if !update.Applied || update.Order.Status != domain.StatusInventoryReserved {
		t.Fatalf("статус не применён: %+v", update)
	}

	if _, err := repo.ApplyStatus(ctx, params); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("err = %v, ожидалось ErrDuplicateEvent", err)
	}
}

// Ключевой регресс: событие, пришедшее не по порядку, не должно откатывать
// заказ назад и не должно теряться.
func TestApplyStatusIgnoresStaleEvent(t *testing.T) {
	pool := newTestPool(t)
	repo := NewPostgresRepository(pool)
	ctx := context.Background()

	userID := uuid.NewString()
	order := newTestOrder(t, userID)
	if err := repo.Create(ctx, order, kafka.NewEvent(kafka.EventOrderCreated, order.ID, userID, nil), "order.events"); err != nil {
		t.Fatalf("create order: %v", err)
	}

	// payment_succeeded пришло раньше inventory_reserved.
	if _, err := repo.ApplyStatus(ctx, ApplyStatusParams{
		OrderID: order.ID, Status: domain.StatusPaid,
		EventID: uuid.NewString(), EventType: kafka.EventPaymentSucceeded,
		ConsumerName: "order-service-test",
	}); err != nil {
		t.Fatalf("применение paid: %v", err)
	}

	// Запоздавшее inventory_reserved должно быть отброшено без ошибки.
	update, err := repo.ApplyStatus(ctx, ApplyStatusParams{
		OrderID: order.ID, Status: domain.StatusInventoryReserved,
		EventID: uuid.NewString(), EventType: kafka.EventInventoryReserved,
		ConsumerName: "order-service-test",
	})
	if err != nil {
		t.Fatalf("применение устаревшего события: %v", err)
	}
	if update.Applied {
		t.Fatal("устаревшее событие не должно применяться")
	}
	if update.Order.Status != domain.StatusPaid {
		t.Fatalf("статус = %s, ожидался paid", update.Order.Status)
	}
}

// Параллельная обработка событий одного заказа не должна нарушать
// монотонность статуса.
func TestConcurrentStatusUpdatesKeepLatestState(t *testing.T) {
	pool := newTestPool(t)
	repo := NewPostgresRepository(pool)
	ctx := context.Background()

	userID := uuid.NewString()
	order := newTestOrder(t, userID)
	if err := repo.Create(ctx, order, kafka.NewEvent(kafka.EventOrderCreated, order.ID, userID, nil), "order.events"); err != nil {
		t.Fatalf("create order: %v", err)
	}

	statuses := []string{
		domain.StatusInventoryReserved,
		domain.StatusPaid,
		domain.StatusProductionStarted,
		domain.StatusCompleted,
	}

	var wg sync.WaitGroup
	wg.Add(len(statuses))
	for _, statusValue := range statuses {
		go func(statusValue string) {
			defer wg.Done()
			_, _ = repo.ApplyStatus(ctx, ApplyStatusParams{
				OrderID: order.ID, Status: statusValue,
				EventID: uuid.NewString(), EventType: "test_event",
				ConsumerName: "order-service-test",
			})
		}(statusValue)
	}
	wg.Wait()

	final, err := repo.GetByIDAnyUser(ctx, order.ID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if final.Status != domain.StatusCompleted {
		t.Fatalf("итоговый статус = %s, ожидался completed", final.Status)
	}
}

func TestCatalogReturnsOnlyActiveItems(t *testing.T) {
	pool := newTestPool(t)
	catalog := NewPostgresCatalogRepository(pool)

	items, err := catalog.FindActiveBySKUs(context.Background(),
		[]string{"ENGINE-X1", "FUEL-TANK", "NOT-A-REAL-SKU"})
	if err != nil {
		t.Fatalf("find catalog items: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("найдено позиций: %d, ожидалось 2", len(items))
	}
	if items["ENGINE-X1"].UnitPrice.String() != "1250.00" {
		t.Fatalf("цена ENGINE-X1 = %s, ожидалось 1250.00", items["ENGINE-X1"].UnitPrice)
	}
	if _, exists := items["NOT-A-REAL-SKU"]; exists {
		t.Fatal("несуществующий SKU не должен возвращаться")
	}
}
