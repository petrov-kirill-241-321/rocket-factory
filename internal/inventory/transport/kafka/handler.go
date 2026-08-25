// Package kafka содержит обработчик доменных событий для inventory-service.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/usecase"
	sharedkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
)

const consumerName = "inventory-service"

// Handler закрывает полный цикл резерва:
//
//	order_created        -> удержать остатки
//	payment_failed       -> вернуть удержание в продажу
//	production_completed -> списать удержание
//
// Раньше сервис слушал только order_created, поэтому quantity_reserved
// монотонно рос и склад исчерпывался независимо от исхода заказов.
type Handler struct {
	inventory *usecase.InventoryUsecase
	logger    *slog.Logger
	metrics   *observability.Metrics
}

func NewHandler(inventory *usecase.InventoryUsecase, logger *slog.Logger, metrics *observability.Metrics) *Handler {
	return &Handler{inventory: inventory, logger: logger, metrics: metrics}
}

func (h *Handler) Handle(ctx context.Context, event sharedkafka.Event) error {
	eventContext := repository.EventContext{
		EventID:      event.EventID,
		EventType:    event.EventType,
		ConsumerName: consumerName,
	}

	switch event.EventType {
	case sharedkafka.EventOrderCreated:
		return h.reserve(ctx, event, eventContext)
	case sharedkafka.EventPaymentFailed:
		return h.settle(ctx, event, eventContext, settleRelease)
	case sharedkafka.EventProductionCompleted:
		return h.settle(ctx, event, eventContext, settleCommit)
	default:
		return sharedkafka.ErrSkip
	}
}

func (h *Handler) reserve(ctx context.Context, event sharedkafka.Event, eventContext repository.EventContext) error {
	items, err := itemsFromPayload(event.Payload)
	if err != nil {
		// Битый payload повтором не исправить — сразу в DLQ.
		return sharedkafka.Permanent(err)
	}

	reservation, err := h.inventory.ReserveForOrder(ctx, event.OrderID, event.UserID, items, eventContext)
	switch {
	case err == nil:
	case errors.Is(err, repository.ErrDuplicateEvent), errors.Is(err, repository.ErrReservationExists):
		return sharedkafka.ErrAlreadyProcessed
	case errors.Is(err, domain.ErrEmptyReservation),
		errors.Is(err, domain.ErrInvalidQuantity),
		errors.Is(err, domain.ErrInvalidSKU):
		return sharedkafka.Permanent(err)
	default:
		return err
	}

	h.metrics.ReservationsTotal.WithLabelValues(reservation.Status).Inc()
	h.logger.InfoContext(ctx, "inventory reservation processed",
		"reservation_id", reservation.ID, "status", reservation.Status, "reason", reservation.Reason)
	return nil
}

type settleKind int

const (
	settleRelease settleKind = iota
	settleCommit
)

func (h *Handler) settle(
	ctx context.Context,
	event sharedkafka.Event,
	eventContext repository.EventContext,
	kind settleKind,
) error {
	var (
		reservation domain.Reservation
		err         error
	)

	if kind == settleRelease {
		reason, _ := event.Payload["reason"].(string)
		if reason == "" {
			reason = "payment failed"
		}
		reservation, err = h.inventory.ReleaseForOrder(ctx, event.OrderID, event.UserID, reason, eventContext)
	} else {
		reservation, err = h.inventory.CommitForOrder(ctx, event.OrderID, event.UserID, eventContext)
	}

	switch {
	case err == nil:
	case errors.Is(err, repository.ErrDuplicateEvent):
		return sharedkafka.ErrAlreadyProcessed
	case errors.Is(err, repository.ErrReservationNotFound):
		// Резерва нет: заказ провалился ещё на этапе склада либо событие
		// относится к чужим данным. Возвращать нечего.
		h.logger.InfoContext(ctx, "no reservation to settle", "order_id", event.OrderID)
		return sharedkafka.ErrStale
	default:
		return err
	}

	h.metrics.ReservationsTotal.WithLabelValues(reservation.Status).Inc()
	h.logger.InfoContext(ctx, "inventory reservation settled",
		"reservation_id", reservation.ID, "status", reservation.Status)
	return nil
}

// itemsFromPayload разбирает позиции заказа из конверта события.
// JSON-числа приходят как float64, поэтому количество проверяется на целостность.
func itemsFromPayload(payload map[string]any) ([]domain.ReservationItem, error) {
	rawItems, ok := payload["items"].([]any)
	if !ok {
		return nil, fmt.Errorf("payload.items is missing or is not an array")
	}
	if len(rawItems) == 0 {
		return nil, fmt.Errorf("payload.items is empty")
	}

	items := make([]domain.ReservationItem, 0, len(rawItems))
	for index, raw := range rawItems {
		itemMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("payload.items[%d] is not an object", index)
		}

		sku, _ := itemMap["sku"].(string)
		if sku == "" {
			return nil, fmt.Errorf("payload.items[%d].sku is missing", index)
		}

		quantityValue, ok := itemMap["quantity"].(float64)
		if !ok {
			return nil, fmt.Errorf("payload.items[%d].quantity is missing or is not a number", index)
		}
		quantity := int(quantityValue)
		if float64(quantity) != quantityValue || quantity <= 0 {
			return nil, fmt.Errorf("payload.items[%d].quantity must be a positive integer", index)
		}

		name, _ := itemMap["name"].(string)
		items = append(items, domain.ReservationItem{SKU: sku, Name: name, Quantity: quantity})
	}
	return items, nil
}
