// Package kafka содержит обработчик доменных событий для order-service.
//
// Чтение из брокера, повторы, дедупликация и DLQ вынесены в общий
// internal/kafka.Consumer — здесь остаётся только бизнес-реакция на событие.
package kafka

import (
	"context"
	"errors"
	"log/slog"

	sharedkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/usecase"
)

const consumerName = "order-service"

type Handler struct {
	orders  *usecase.OrderUsecase
	logger  *slog.Logger
	metrics *observability.Metrics
}

func NewHandler(orders *usecase.OrderUsecase, logger *slog.Logger, metrics *observability.Metrics) *Handler {
	return &Handler{orders: orders, logger: logger, metrics: metrics}
}

func (h *Handler) Handle(ctx context.Context, event sharedkafka.Event) error {
	statusValue, ok := orderStatusForEvent(event.EventType)
	if !ok {
		return sharedkafka.ErrSkip
	}

	update, err := h.orders.ApplyEventStatus(ctx, event, statusValue, consumerName)
	switch {
	case err == nil:
	case errors.Is(err, repository.ErrDuplicateEvent):
		return sharedkafka.ErrAlreadyProcessed

	case errors.Is(err, repository.ErrOrderNotFound):
		// Заказ отсутствует: событие относится к чужим или удалённым данным.
		// Повтор ничего не изменит — отправляем в DLQ для разбора.
		return sharedkafka.Permanent(err)

	case errors.Is(err, domain.ErrUnknownStatus):
		return sharedkafka.Permanent(err)

	default:
		// Транзиентная ошибка (недоступность БД, взаимная блокировка) —
		// общий консьюмер повторит попытку с нарастающей паузой.
		return err
	}

	if !update.Applied {
		// Событие пришло не по порядку и уже неактуально: заказ находится
		// в более позднем состоянии. Отбрасываем без повтора.
		h.logger.InfoContext(ctx, "order event superseded by later state",
			"current_status", update.PreviousStatus, "incoming_status", statusValue)
		return sharedkafka.ErrStale
	}

	h.metrics.OrderStatusApplied.WithLabelValues(update.PreviousStatus, statusValue).Inc()
	h.logger.InfoContext(ctx, "order status applied",
		"previous_status", update.PreviousStatus, "status", statusValue)
	return nil
}

func orderStatusForEvent(eventType string) (string, bool) {
	switch eventType {
	case sharedkafka.EventInventoryReserved:
		return domain.StatusInventoryReserved, true
	case sharedkafka.EventInventoryFailed:
		return domain.StatusInventoryFailed, true
	case sharedkafka.EventPaymentSucceeded:
		return domain.StatusPaid, true
	case sharedkafka.EventPaymentFailed:
		return domain.StatusFailed, true
	case sharedkafka.EventProductionStarted:
		return domain.StatusProductionStarted, true
	case sharedkafka.EventProductionCompleted:
		return domain.StatusCompleted, true
	default:
		return "", false
	}
}
