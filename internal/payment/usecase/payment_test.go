package usecase

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/repository"
)

func newUsecase(orderStatus string) (*PaymentUsecase, *memoryRepository, *memoryIdempotency, *memoryOrderGateway) {
	payments := newMemoryRepository()
	idempotency := newMemoryIdempotency()
	amount, _ := money.Parse("2920.00")
	orders := &memoryOrderGateway{order: OrderSnapshot{
		ID:          "order-1",
		UserID:      "user-1",
		Status:      orderStatus,
		TotalAmount: amount,
	}}
	return NewPaymentUsecase(payments, idempotency, orders, "payment.events"), payments, idempotency, orders
}

func TestCreatePaymentPublishesSucceededEvent(t *testing.T) {
	uc, payments, _, _ := newUsecase("inventory_reserved")

	payment, err := uc.Create(context.Background(), CreatePaymentInput{
		OrderID:        "order-1",
		UserID:         "user-1",
		IdempotencyKey: "idem-1",
		Simulation:     SimulationSuccess,
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}

	if payment.Status != domain.StatusSucceeded {
		t.Fatalf("статус = %s, ожидался %s", payment.Status, domain.StatusSucceeded)
	}
	// Сумма берётся из заказа, а не из запроса.
	if payment.Amount.String() != "2920.00" {
		t.Fatalf("сумма = %s, ожидалось 2920.00", payment.Amount)
	}
	if len(payments.events) != 1 {
		t.Fatalf("событий = %d, ожидалось 1", len(payments.events))
	}
	if payments.events[0].EventType != kafka.EventPaymentSucceeded {
		t.Fatalf("тип события = %s, ожидался %s", payments.events[0].EventType, kafka.EventPaymentSucceeded)
	}
}

func TestCreatePaymentPublishesFailedEvent(t *testing.T) {
	uc, payments, _, _ := newUsecase("inventory_reserved")

	payment, err := uc.Create(context.Background(), CreatePaymentInput{
		OrderID:    "order-1",
		UserID:     "user-1",
		Simulation: SimulationFailure,
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}

	if payment.Status != domain.StatusFailed {
		t.Fatalf("статус = %s, ожидался %s", payment.Status, domain.StatusFailed)
	}
	if payments.events[0].EventType != kafka.EventPaymentFailed {
		t.Fatalf("тип события = %s, ожидался %s", payments.events[0].EventType, kafka.EventPaymentFailed)
	}
	// Причина нужна inventory-service, чтобы вернуть удержанные остатки.
	if _, ok := payments.events[0].Payload["reason"]; !ok {
		t.Fatal("в событии payment_failed отсутствует причина")
	}
}

func TestCreatePaymentDuplicateIdempotencyReturnsExistingPayment(t *testing.T) {
	uc, payments, _, _ := newUsecase("inventory_reserved")
	ctx := context.Background()

	input := CreatePaymentInput{
		OrderID:        "order-1",
		UserID:         "user-1",
		IdempotencyKey: "idem-1",
		Simulation:     SimulationSuccess,
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
		t.Fatalf("повтор создал новый платёж %s вместо %s", second.ID, first.ID)
	}
	if len(payments.events) != 1 {
		t.Fatalf("событий = %d, ожидалось 1", len(payments.events))
	}
}

// Регресс на двойное списание: без Idempotency-Key повторная оплата заказа
// должна отклоняться уникальным индексом активного платежа.
func TestSecondPaymentWithoutIdempotencyKeyIsRejected(t *testing.T) {
	uc, payments, _, _ := newUsecase("inventory_reserved")
	ctx := context.Background()

	input := CreatePaymentInput{OrderID: "order-1", UserID: "user-1", Simulation: SimulationSuccess}

	first, err := uc.Create(ctx, input)
	if err != nil {
		t.Fatalf("первая оплата: %v", err)
	}

	second, err := uc.Create(ctx, input)
	if err != nil {
		t.Fatalf("вторая оплата вернула ошибку вместо существующего платежа: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("создан второй платёж %s: заказ оплачен дважды", second.ID)
	}
	if count := payments.count(); count != 1 {
		t.Fatalf("платежей в хранилище = %d, ожидался 1", count)
	}
}

// Параллельные запросы на оплату одного заказа не должны приводить
// к нескольким успешным платежам.
func TestConcurrentPaymentsCreateSinglePayment(t *testing.T) {
	uc, payments, _, _ := newUsecase("inventory_reserved")

	const attempts = 8
	var wg sync.WaitGroup
	wg.Add(attempts)

	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			_, _ = uc.Create(context.Background(), CreatePaymentInput{
				OrderID:    "order-1",
				UserID:     "user-1",
				Simulation: SimulationSuccess,
			})
		}()
	}
	wg.Wait()

	if count := payments.count(); count != 1 {
		t.Fatalf("создано платежей: %d, ожидался ровно 1", count)
	}
}

func TestPaymentRejectedForNonPayableOrder(t *testing.T) {
	for _, status := range []string{"created", "paid", "completed", "failed", "inventory_failed"} {
		t.Run(status, func(t *testing.T) {
			uc, _, _, _ := newUsecase(status)

			_, err := uc.Create(context.Background(), CreatePaymentInput{
				OrderID: "order-1", UserID: "user-1", Simulation: SimulationSuccess,
			})
			if !errors.Is(err, ErrOrderNotPayable) {
				t.Fatalf("err = %v, ожидалось ErrOrderNotPayable", err)
			}
		})
	}
}

// Регресс: отсутствующий заказ должен давать понятную ошибку,
// а не превращаться во внутреннюю ошибку сервиса.
func TestMissingOrderIsReportedAsNotFound(t *testing.T) {
	payments := newMemoryRepository()
	uc := NewPaymentUsecase(payments, newMemoryIdempotency(),
		&memoryOrderGateway{err: ErrOrderNotFound}, "payment.events")

	_, err := uc.Create(context.Background(), CreatePaymentInput{
		OrderID: "missing", UserID: "user-1",
	})
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("err = %v, ожидалось ErrOrderNotFound", err)
	}
}

// Регресс: при ошибке ключ идемпотентности должен освобождаться.
func TestFailedPaymentReleasesIdempotencyKey(t *testing.T) {
	uc, _, idempotency, orders := newUsecase("inventory_reserved")
	orders.err = errors.New("order-service недоступен")

	_, err := uc.Create(context.Background(), CreatePaymentInput{
		OrderID: "order-1", UserID: "user-1", IdempotencyKey: "idem-retry",
	})
	if err == nil {
		t.Fatal("ожидалась ошибка")
	}

	if idempotency.isHeld("idempotency:payments:user-1:order-1:idem-retry") {
		t.Fatal("ключ идемпотентности остался занят после ошибки")
	}
}

// --- тестовые двойники ---

type memoryRepository struct {
	mu        sync.Mutex
	payments  map[string]domain.Payment
	idemIndex map[string]string
	active    map[string]string
	events    []kafka.Event
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		payments:  map[string]domain.Payment{},
		idemIndex: map[string]string{},
		active:    map[string]string{},
	}
}

func (r *memoryRepository) Create(_ context.Context, payment domain.Payment, event kafka.Event, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if payment.IdempotencyKey != "" {
		key := payment.OrderID + ":" + payment.UserID + ":" + payment.IdempotencyKey
		if _, exists := r.idemIndex[key]; exists {
			return repository.ErrDuplicateIdempotKey
		}
	}
	// Соответствует частичному уникальному индексу payments_order_id_active_uq.
	if payment.IsActive() {
		if _, exists := r.active[payment.OrderID]; exists {
			return repository.ErrActivePaymentExists
		}
		r.active[payment.OrderID] = payment.ID
	}
	if payment.IdempotencyKey != "" {
		r.idemIndex[payment.OrderID+":"+payment.UserID+":"+payment.IdempotencyKey] = payment.ID
	}

	r.payments[payment.ID] = payment
	r.events = append(r.events, event)
	return nil
}

func (r *memoryRepository) GetByID(_ context.Context, paymentID string) (domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	payment, ok := r.payments[paymentID]
	if !ok {
		return domain.Payment{}, repository.ErrPaymentNotFound
	}
	return payment, nil
}

func (r *memoryRepository) FindByIdempotencyKey(_ context.Context, orderID, userID, key string) (domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	paymentID, ok := r.idemIndex[orderID+":"+userID+":"+key]
	if !ok {
		return domain.Payment{}, repository.ErrPaymentNotFound
	}
	return r.payments[paymentID], nil
}

func (r *memoryRepository) FindActiveByOrder(_ context.Context, orderID, userID string) (domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	paymentID, ok := r.active[orderID]
	if !ok {
		return domain.Payment{}, repository.ErrPaymentNotFound
	}
	payment := r.payments[paymentID]
	if payment.UserID != userID {
		return domain.Payment{}, repository.ErrPaymentNotFound
	}
	return payment, nil
}

func (r *memoryRepository) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.payments)
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

type memoryOrderGateway struct {
	order OrderSnapshot
	err   error
}

func (g *memoryOrderGateway) GetOrder(_ context.Context, _, _ string) (OrderSnapshot, error) {
	if g.err != nil {
		return OrderSnapshot{}, g.err
	}
	return g.order, nil
}
