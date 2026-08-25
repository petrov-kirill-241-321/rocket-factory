package usecase

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/repository"
)

func newUsecase() (*OrderUsecase, *memoryRepository, *memoryIdempotency) {
	orders := newMemoryRepository()
	idempotency := newMemoryIdempotency()
	return NewOrderUsecase(orders, stubCatalog{}, idempotency, "order.events"), orders, idempotency
}

// Цена берётся из каталога: клиент передаёт только SKU и количество.
func TestCreateOrderUsesCatalogPrice(t *testing.T) {
	uc, orders, _ := newUsecase()

	order, err := uc.Create(context.Background(), CreateOrderInput{
		UserID:         "user-1",
		IdempotencyKey: "idem-1",
		Items: []CreateOrderItem{
			{SKU: "ENGINE-X1", Quantity: 2},
			{SKU: "FUEL-TANK", Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}

	if order.Status != domain.StatusCreated {
		t.Fatalf("статус = %s, ожидался %s", order.Status, domain.StatusCreated)
	}
	// 2 * 1250.00 + 1 * 420.00
	if order.TotalAmount.String() != "2920.00" {
		t.Fatalf("сумма = %s, ожидалось 2920.00", order.TotalAmount)
	}
	if len(orders.events) != 1 || orders.events[0].EventType != kafka.EventOrderCreated {
		t.Fatalf("ожидалось одно событие order_created, получено %+v", orders.events)
	}
}

func TestCreateOrderRejectsUnknownSKU(t *testing.T) {
	uc, _, _ := newUsecase()

	_, err := uc.Create(context.Background(), CreateOrderInput{
		UserID: "user-1",
		Items:  []CreateOrderItem{{SKU: "NOT-A-REAL-SKU", Quantity: 1}},
	})
	if !errors.Is(err, ErrUnknownSKU) {
		t.Fatalf("err = %v, ожидалось ErrUnknownSKU", err)
	}
}

func TestCreateOrderValidatesInput(t *testing.T) {
	uc, _, _ := newUsecase()
	ctx := context.Background()

	cases := map[string]struct {
		items []CreateOrderItem
		want  error
	}{
		"пустой заказ":         {nil, ErrEmptyOrder},
		"нулевое количество":   {[]CreateOrderItem{{SKU: "ENGINE-X1", Quantity: 0}}, ErrInvalidQuantity},
		"отрицательное":        {[]CreateOrderItem{{SKU: "ENGINE-X1", Quantity: -5}}, ErrInvalidQuantity},
		"пустой sku":           {[]CreateOrderItem{{SKU: "   ", Quantity: 1}}, ErrInvalidItem},
		"дубли sku":            {[]CreateOrderItem{{SKU: "ENGINE-X1", Quantity: 1}, {SKU: "ENGINE-X1", Quantity: 2}}, ErrDuplicateSKU},
		"слишком много позиций": {make([]CreateOrderItem, domain.MaxItemsPerOrder+1), ErrTooManyItems},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := uc.Create(ctx, CreateOrderInput{UserID: "user-1", Items: tc.items})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, ожидалось %v", err, tc.want)
			}
		})
	}
}

func TestCreateOrderReturnsExistingOrderForDuplicateKey(t *testing.T) {
	uc, orders, _ := newUsecase()
	ctx := context.Background()

	input := CreateOrderInput{
		UserID:         "user-1",
		IdempotencyKey: "idem-1",
		Items:          []CreateOrderItem{{SKU: "FUEL-TANK", Quantity: 1}},
	}

	first, err := uc.Create(ctx, input)
	if err != nil {
		t.Fatalf("первый вызов: %v", err)
	}
	second, err := uc.Create(ctx, input)
	if err != nil {
		t.Fatalf("второй вызов: %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("повтор создал новый заказ %s вместо %s", second.ID, first.ID)
	}
	if len(orders.events) != 1 {
		t.Fatalf("событий = %d, ожидалось 1", len(orders.events))
	}
}

// Регресс: раньше ключ помечался использованным даже при ошибке, и клиент
// не мог повторить запрос до истечения TTL, получая 404.
func TestFailedCreateReleasesIdempotencyKey(t *testing.T) {
	uc, orders, idempotency := newUsecase()
	ctx := context.Background()

	orders.failNext = errors.New("база данных недоступна")

	input := CreateOrderInput{
		UserID:         "user-1",
		IdempotencyKey: "idem-retry",
		Items:          []CreateOrderItem{{SKU: "ENGINE-X1", Quantity: 1}},
	}

	if _, err := uc.Create(ctx, input); err == nil {
		t.Fatal("первый вызов должен был завершиться ошибкой")
	}

	if idempotency.isHeld("idempotency:orders:user-1:idem-retry") {
		t.Fatal("ключ идемпотентности остался занят после ошибки")
	}

	// Повтор с тем же ключом обязан создать заказ, а не вернуть 404.
	order, err := uc.Create(ctx, input)
	if err != nil {
		t.Fatalf("повтор после ошибки: %v", err)
	}
	if order.ID == "" {
		t.Fatal("повтор не создал заказ")
	}
}

// Регресс: параллельный запрос с тем же ключом раньше получал 404,
// потому что заказ ещё не был записан в БД.
func TestConcurrentRequestWithSameKeyReportsInFlight(t *testing.T) {
	uc, _, idempotency := newUsecase()

	// Имитируем состояние «ключ захвачен, заказа ещё нет».
	_, _ = idempotency.Claim(context.Background(), "idempotency:orders:user-1:idem-race")

	_, err := uc.Create(context.Background(), CreateOrderInput{
		UserID:         "user-1",
		IdempotencyKey: "idem-race",
		Items:          []CreateOrderItem{{SKU: "ENGINE-X1", Quantity: 1}},
	})
	if !errors.Is(err, ErrIdempotencyInFlight) {
		t.Fatalf("err = %v, ожидалось ErrIdempotencyInFlight", err)
	}
}

// Недоступность Redis не должна мешать созданию заказов: гарантию
// уникальности обеспечивает индекс в PostgreSQL.
func TestCreateOrderWorksWhenIdempotencyStoreIsDown(t *testing.T) {
	orders := newMemoryRepository()
	uc := NewOrderUsecase(orders, stubCatalog{}, brokenIdempotency{}, "order.events")

	order, err := uc.Create(context.Background(), CreateOrderInput{
		UserID:         "user-1",
		IdempotencyKey: "idem-1",
		Items:          []CreateOrderItem{{SKU: "ENGINE-X1", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("создание заказа при недоступном Redis: %v", err)
	}
	if order.ID == "" {
		t.Fatal("заказ не создан")
	}
}

// Позиции должны уходить в событие в стабильном порядке: от него зависит
// порядок блокировки строк склада и, значит, отсутствие взаимных блокировок.
func TestItemsAreSortedBySKU(t *testing.T) {
	uc, _, _ := newUsecase()

	order, err := uc.Create(context.Background(), CreateOrderInput{
		UserID: "user-1",
		Items: []CreateOrderItem{
			{SKU: "FUEL-TANK", Quantity: 1},
			{SKU: "ENGINE-X1", Quantity: 1},
			{SKU: "HULL-AERO", Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}

	expected := []string{"ENGINE-X1", "FUEL-TANK", "HULL-AERO"}
	for i, sku := range expected {
		if order.Items[i].SKU != sku {
			t.Fatalf("позиция %d = %s, ожидалась %s", i, order.Items[i].SKU, sku)
		}
	}
}

// --- тестовые двойники ---

type stubCatalog struct{}

func (stubCatalog) FindActiveBySKUs(_ context.Context, skus []string) (map[string]domain.CatalogItem, error) {
	prices := map[string]string{
		"ENGINE-X1":  "1250.00",
		"FUEL-TANK":  "420.00",
		"NAV-MODULE": "890.00",
		"HULL-AERO":  "2100.00",
	}

	out := make(map[string]domain.CatalogItem)
	for _, sku := range skus {
		price, ok := prices[sku]
		if !ok {
			continue
		}
		amount, err := money.ParsePositive(price)
		if err != nil {
			return nil, err
		}
		out[sku] = domain.CatalogItem{SKU: sku, Name: sku, UnitPrice: amount}
	}
	return out, nil
}

type memoryRepository struct {
	mu        sync.Mutex
	orders    map[string]domain.Order
	idemIndex map[string]string
	events    []kafka.Event
	failNext  error
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		orders:    map[string]domain.Order{},
		idemIndex: map[string]string{},
	}
}

func (r *memoryRepository) Create(_ context.Context, order domain.Order, event kafka.Event, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failNext != nil {
		err := r.failNext
		r.failNext = nil
		return err
	}

	if order.IdempotencyKey != "" {
		key := order.UserID + ":" + order.IdempotencyKey
		if _, exists := r.idemIndex[key]; exists {
			return repository.ErrDuplicateIdempotKey
		}
		r.idemIndex[key] = order.ID
	}
	r.orders[order.ID] = order
	r.events = append(r.events, event)
	return nil
}

func (r *memoryRepository) GetByID(_ context.Context, orderID, userID string) (domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	order, ok := r.orders[orderID]
	if !ok || order.UserID != userID {
		return domain.Order{}, repository.ErrOrderNotFound
	}
	return order, nil
}

func (r *memoryRepository) GetByIDAnyUser(_ context.Context, orderID string) (domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	order, ok := r.orders[orderID]
	if !ok {
		return domain.Order{}, repository.ErrOrderNotFound
	}
	return order, nil
}

func (r *memoryRepository) ListByUser(_ context.Context, userID string, _, _ int) ([]domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]domain.Order, 0)
	for _, order := range r.orders {
		if order.UserID == userID {
			out = append(out, order)
		}
	}
	return out, nil
}

func (r *memoryRepository) FindByIdempotencyKey(_ context.Context, userID, key string) (domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	orderID, ok := r.idemIndex[userID+":"+key]
	if !ok {
		return domain.Order{}, repository.ErrOrderNotFound
	}
	return r.orders[orderID], nil
}

func (r *memoryRepository) ApplyStatus(_ context.Context, params repository.ApplyStatusParams) (repository.StatusUpdate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	order, ok := r.orders[params.OrderID]
	if !ok {
		return repository.StatusUpdate{}, repository.ErrOrderNotFound
	}

	previous := order.Status
	apply, err := domain.ShouldApply(previous, params.Status)
	if err != nil {
		return repository.StatusUpdate{}, err
	}
	if !apply {
		return repository.StatusUpdate{Order: order, PreviousStatus: previous, Applied: false}, nil
	}

	order.Status = params.Status
	r.orders[params.OrderID] = order
	return repository.StatusUpdate{Order: order, PreviousStatus: previous, Applied: true}, nil
}

type memoryIdempotency struct {
	mu   sync.Mutex
	keys map[string]string
}

func newMemoryIdempotency() *memoryIdempotency {
	return &memoryIdempotency{keys: map[string]string{}}
}

func (s *memoryIdempotency) Claim(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.keys[key]; exists {
		return false, nil
	}
	s.keys[key] = "in_progress"
	return true, nil
}

func (s *memoryIdempotency) MarkDone(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.keys[key] = "done"
	return nil
}

func (s *memoryIdempotency) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.keys, key)
	return nil
}

func (s *memoryIdempotency) isHeld(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.keys[key]
	return exists
}

// brokenIdempotency имитирует недоступный Redis.
type brokenIdempotency struct{}

func (brokenIdempotency) Claim(context.Context, string) (bool, error) {
	return false, errors.New("redis недоступен")
}
func (brokenIdempotency) MarkDone(context.Context, string) error { return errors.New("redis недоступен") }
func (brokenIdempotency) Release(context.Context, string) error  { return errors.New("redis недоступен") }
