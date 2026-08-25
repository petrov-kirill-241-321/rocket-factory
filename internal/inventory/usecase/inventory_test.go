package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
)

func TestReserveForOrderPublishesReservedEvent(t *testing.T) {
	repo := newMemoryRepository()
	uc := NewInventoryUsecase(repo, "inventory.events")

	reservation, err := uc.ReserveForOrder(context.Background(), "order-1", "user-1",
		[]domain.ReservationItem{{SKU: "ENGINE-X1", Name: "Engine X1", Quantity: 1}},
		repository.EventContext{EventID: "event-1", EventType: kafka.EventOrderCreated, ConsumerName: "inventory-service"},
	)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	if reservation.Status != domain.ReservationStatusReserved {
		t.Fatalf("статус = %s, ожидался %s", reservation.Status, domain.ReservationStatusReserved)
	}
	if len(repo.events) != 1 || repo.events[0].EventType != kafka.EventInventoryReserved {
		t.Fatalf("ожидалось событие inventory_reserved, получено %+v", repo.events)
	}
}

// Полный цикл: удержание -> возврат в продажу. Без этого перехода остатки
// «вытекали» при каждой неуспешной оплате.
func TestReleaseReturnsStockBack(t *testing.T) {
	repo := newMemoryRepository()
	uc := NewInventoryUsecase(repo, "inventory.events")
	ctx := context.Background()

	items := []domain.ReservationItem{{SKU: "ENGINE-X1", Quantity: 3}}
	if _, err := uc.ReserveForOrder(ctx, "order-1", "user-1", items,
		repository.EventContext{EventID: "event-1"}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if repo.reserved["ENGINE-X1"] != 3 {
		t.Fatalf("удержано = %d, ожидалось 3", repo.reserved["ENGINE-X1"])
	}

	reservation, err := uc.ReleaseForOrder(ctx, "order-1", "user-1", "payment failed",
		repository.EventContext{EventID: "event-2"})
	if err != nil {
		t.Fatalf("release: %v", err)
	}

	if reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("статус = %s, ожидался %s", reservation.Status, domain.ReservationStatusReleased)
	}
	if repo.reserved["ENGINE-X1"] != 0 {
		t.Fatalf("удержание не снято: осталось %d", repo.reserved["ENGINE-X1"])
	}
	if repo.available["ENGINE-X1"] != 10 {
		t.Fatalf("остаток = %d, ожидалось 10", repo.available["ENGINE-X1"])
	}
}

// Завершение производства должно списывать удержанное количество.
func TestCommitConsumesStock(t *testing.T) {
	repo := newMemoryRepository()
	uc := NewInventoryUsecase(repo, "inventory.events")
	ctx := context.Background()

	items := []domain.ReservationItem{{SKU: "ENGINE-X1", Quantity: 2}}
	if _, err := uc.ReserveForOrder(ctx, "order-1", "user-1", items,
		repository.EventContext{EventID: "event-1"}); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	reservation, err := uc.CommitForOrder(ctx, "order-1", "user-1",
		repository.EventContext{EventID: "event-2"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if reservation.Status != domain.ReservationStatusCommitted {
		t.Fatalf("статус = %s, ожидался %s", reservation.Status, domain.ReservationStatusCommitted)
	}
	if repo.reserved["ENGINE-X1"] != 0 {
		t.Fatalf("удержание не снято: осталось %d", repo.reserved["ENGINE-X1"])
	}
	if repo.available["ENGINE-X1"] != 8 {
		t.Fatalf("остаток = %d, ожидалось 8", repo.available["ENGINE-X1"])
	}
}

// Повторная доставка события не должна списывать остатки дважды.
func TestSettleIsIdempotent(t *testing.T) {
	repo := newMemoryRepository()
	uc := NewInventoryUsecase(repo, "inventory.events")
	ctx := context.Background()

	if _, err := uc.ReserveForOrder(ctx, "order-1", "user-1",
		[]domain.ReservationItem{{SKU: "ENGINE-X1", Quantity: 2}},
		repository.EventContext{EventID: "event-1"}); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := uc.CommitForOrder(ctx, "order-1", "user-1", repository.EventContext{}); err != nil {
			t.Fatalf("commit #%d: %v", i, err)
		}
	}

	if repo.available["ENGINE-X1"] != 8 {
		t.Fatalf("остаток = %d, ожидалось 8: повторные события списали лишнее", repo.available["ENGINE-X1"])
	}
}

func TestReserveFailsWhenStockIsInsufficient(t *testing.T) {
	repo := newMemoryRepository()
	uc := NewInventoryUsecase(repo, "inventory.events")

	reservation, err := uc.ReserveForOrder(context.Background(), "order-1", "user-1",
		[]domain.ReservationItem{{SKU: "ENGINE-X1", Quantity: 999}},
		repository.EventContext{EventID: "event-1"})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	if reservation.Status != domain.ReservationStatusFailed {
		t.Fatalf("статус = %s, ожидался %s", reservation.Status, domain.ReservationStatusFailed)
	}
	if repo.reserved["ENGINE-X1"] != 0 {
		t.Fatal("при неуспешном резерве остатки не должны меняться")
	}
	if repo.events[0].EventType != kafka.EventInventoryFailed {
		t.Fatalf("тип события = %s, ожидался %s", repo.events[0].EventType, kafka.EventInventoryFailed)
	}
}

func TestReserveValidatesItems(t *testing.T) {
	uc := NewInventoryUsecase(newMemoryRepository(), "inventory.events")
	ctx := context.Background()

	if _, err := uc.ReserveForOrder(ctx, "order-1", "user-1", nil, repository.EventContext{}); !errors.Is(err, ErrEmptyReservation) {
		t.Fatalf("err = %v, ожидалось ErrEmptyReservation", err)
	}

	_, err := uc.ReserveForOrder(ctx, "order-1", "user-1",
		[]domain.ReservationItem{{SKU: "ENGINE-X1", Quantity: 0}}, repository.EventContext{})
	if !errors.Is(err, ErrInvalidQuantity) {
		t.Fatalf("err = %v, ожидалось ErrInvalidQuantity", err)
	}
}

// Позиции должны блокироваться в едином порядке независимо от порядка
// в запросе — иначе параллельные резервы уходят во взаимную блокировку.
func TestItemsAreNormalizedAndSorted(t *testing.T) {
	items, err := domain.NormalizeItems([]domain.ReservationItem{
		{SKU: "fuel-tank", Quantity: 1},
		{SKU: "ENGINE-X1", Quantity: 2},
		{SKU: " engine-x1 ", Quantity: 3},
	})
	if err != nil {
		t.Fatalf("NormalizeItems: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("позиций = %d, ожидалось 2 (дубли объединяются)", len(items))
	}
	if items[0].SKU != "ENGINE-X1" || items[0].Quantity != 5 {
		t.Fatalf("первая позиция = %+v, ожидалось ENGINE-X1 x5", items[0])
	}
	if items[1].SKU != "FUEL-TANK" {
		t.Fatalf("вторая позиция = %s, ожидалось FUEL-TANK", items[1].SKU)
	}
}

// --- тестовый двойник склада ---

type memoryRepository struct {
	available    map[string]int
	reserved     map[string]int
	reservations map[string]domain.Reservation
	processed    map[string]bool
	events       []kafka.Event
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		available:    map[string]int{"ENGINE-X1": 10, "FUEL-TANK": 10},
		reserved:     map[string]int{},
		reservations: map[string]domain.Reservation{},
		processed:    map[string]bool{},
	}
}

func (r *memoryRepository) CheckAvailability(_ context.Context, items []domain.ReservationItem) ([]domain.Availability, error) {
	out := make([]domain.Availability, 0, len(items))
	for _, item := range items {
		free := r.available[item.SKU] - r.reserved[item.SKU]
		out = append(out, domain.Availability{
			SKU: item.SKU, Requested: item.Quantity, Available: free, Enough: free >= item.Quantity,
		})
	}
	return out, nil
}

func (r *memoryRepository) Reserve(_ context.Context, params repository.ReserveParams) (domain.Reservation, error) {
	if err := r.markProcessed(params.Event); err != nil {
		return domain.Reservation{}, err
	}

	reservation := params.Reservation
	event := params.OutboxEvent
	reservation.Status = domain.ReservationStatusReserved

	for _, item := range reservation.Items {
		if r.available[item.SKU]-r.reserved[item.SKU] < item.Quantity {
			reservation.Status = domain.ReservationStatusFailed
			reservation.Reason = "not enough inventory for sku " + item.SKU
			break
		}
	}

	if reservation.Status == domain.ReservationStatusReserved {
		for _, item := range reservation.Items {
			r.reserved[item.SKU] += item.Quantity
		}
	} else {
		event.EventType = kafka.EventInventoryFailed
	}

	r.reservations[reservation.OrderID] = reservation
	r.events = append(r.events, event)
	return reservation, nil
}

func (r *memoryRepository) Release(_ context.Context, params repository.SettleParams) (domain.Reservation, error) {
	return r.settle(params, domain.ReservationStatusReleased, false)
}

func (r *memoryRepository) Commit(_ context.Context, params repository.SettleParams) (domain.Reservation, error) {
	return r.settle(params, domain.ReservationStatusCommitted, true)
}

func (r *memoryRepository) settle(params repository.SettleParams, target string, consume bool) (domain.Reservation, error) {
	if err := r.markProcessed(params.Event); err != nil {
		return domain.Reservation{}, err
	}

	reservation, ok := r.reservations[params.OrderID]
	if !ok {
		return domain.Reservation{}, repository.ErrReservationNotFound
	}
	if reservation.Status != domain.ReservationStatusReserved {
		return reservation, nil
	}

	for _, item := range reservation.Items {
		r.reserved[item.SKU] -= item.Quantity
		if consume {
			r.available[item.SKU] -= item.Quantity
		}
	}

	reservation.Status = target
	r.reservations[params.OrderID] = reservation

	event := params.OutboxEvent
	r.events = append(r.events, event)
	return reservation, nil
}

func (r *memoryRepository) markProcessed(event repository.EventContext) error {
	if event.EventID == "" {
		return nil
	}
	if r.processed[event.EventID] {
		return repository.ErrDuplicateEvent
	}
	r.processed[event.EventID] = true
	return nil
}
