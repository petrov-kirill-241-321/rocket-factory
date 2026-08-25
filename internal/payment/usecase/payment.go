package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/repository"
)

var (
	ErrOrderNotPayable     = errors.New("order is not payable")
	ErrOrderNotFound       = errors.New("order not found")
	ErrPaymentNotFound     = repository.ErrPaymentNotFound
	ErrActivePaymentExists = repository.ErrActivePaymentExists
	ErrIdempotencyInFlight = errors.New("request with the same idempotency key is in flight")
)

const (
	SimulationSuccess = "success"
	SimulationFailure = "failure"
)

type IdempotencyStore interface {
	Claim(ctx context.Context, key string) (bool, error)
	MarkDone(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
}

// OrderGateway читает заказ во внешнем сервисе.
type OrderGateway interface {
	GetOrder(ctx context.Context, orderID, userID string) (OrderSnapshot, error)
}

type OrderSnapshot struct {
	ID          string
	UserID      string
	Status      string
	TotalAmount money.Amount
}

type PaymentUsecase struct {
	payments     repository.Repository
	idempotency  IdempotencyStore
	orders       OrderGateway
	paymentTopic string
}

type CreatePaymentInput struct {
	OrderID        string
	UserID         string
	IdempotencyKey string
	Simulation     string
}

func NewPaymentUsecase(
	payments repository.Repository,
	idempotency IdempotencyStore,
	orders OrderGateway,
	paymentTopic string,
) *PaymentUsecase {
	return &PaymentUsecase{payments: payments, idempotency: idempotency, orders: orders, paymentTopic: paymentTopic}
}

func (u *PaymentUsecase) Create(ctx context.Context, input CreatePaymentInput) (payment domain.Payment, err error) {
	if input.Simulation == "" {
		input.Simulation = SimulationSuccess
	}

	if input.IdempotencyKey != "" {
		key := idempotencyKey(input)

		claimed, claimErr := u.idempotency.Claim(ctx, key)
		if claimErr != nil {
			// Redis недоступен: полагаемся на уникальные индексы в PostgreSQL.
			claimed = true
		}
		if !claimed {
			existing, findErr := u.payments.FindByIdempotencyKey(ctx, input.OrderID, input.UserID, input.IdempotencyKey)
			if findErr == nil {
				return existing, nil
			}
			if errors.Is(findErr, repository.ErrPaymentNotFound) {
				return domain.Payment{}, ErrIdempotencyInFlight
			}
			return domain.Payment{}, findErr
		}

		// Ключ снимается при ошибке, иначе клиент не сможет повторить запрос
		// до истечения TTL и получит 404 вместо повторной попытки.
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()

			if err != nil {
				_ = u.idempotency.Release(releaseCtx, key)
				return
			}
			_ = u.idempotency.MarkDone(releaseCtx, key)
		}()
	}

	order, err := u.orders.GetOrder(ctx, input.OrderID, input.UserID)
	if err != nil {
		return domain.Payment{}, err
	}
	if !domain.PayableOrderStatuses[order.Status] {
		return domain.Payment{}, ErrOrderNotPayable
	}

	statusValue := domain.StatusSucceeded
	eventType := kafka.EventPaymentSucceeded
	if input.Simulation == SimulationFailure {
		statusValue = domain.StatusFailed
		eventType = kafka.EventPaymentFailed
	}

	now := time.Now().UTC()
	// Сумма берётся из заказа, а не из запроса: клиент не управляет тем,
	// сколько с него списывают.
	payment = domain.Payment{
		ID:             uuid.NewString(),
		OrderID:        input.OrderID,
		UserID:         input.UserID,
		Status:         statusValue,
		Amount:         order.TotalAmount,
		IdempotencyKey: input.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	event := kafka.NewEvent(eventType, input.OrderID, input.UserID, map[string]any{
		"payment_id": payment.ID,
		"amount":     payment.Amount.String(),
		"status":     payment.Status,
	})
	if statusValue == domain.StatusFailed {
		event.Payload["reason"] = "payment simulation returned failure"
	}

	if createErr := u.payments.Create(ctx, payment, event, u.paymentTopic); createErr != nil {
		switch {
		case errors.Is(createErr, repository.ErrDuplicateIdempotKey) && input.IdempotencyKey != "":
			return u.payments.FindByIdempotencyKey(ctx, input.OrderID, input.UserID, input.IdempotencyKey)

		case errors.Is(createErr, repository.ErrActivePaymentExists):
			// Параллельный запрос успел раньше. Возвращаем его платёж, если он
			// принадлежит тому же пользователю; иначе — отказ.
			existing, findErr := u.payments.FindActiveByOrder(ctx, input.OrderID, input.UserID)
			if findErr == nil {
				return existing, nil
			}
			return domain.Payment{}, ErrActivePaymentExists

		default:
			return domain.Payment{}, createErr
		}
	}

	return payment, nil
}

func (u *PaymentUsecase) Get(ctx context.Context, paymentID string) (domain.Payment, error) {
	return u.payments.GetByID(ctx, paymentID)
}

func idempotencyKey(input CreatePaymentInput) string {
	return "idempotency:payments:" + input.UserID + ":" + input.OrderID + ":" + input.IdempotencyKey
}
