// Package kafka содержит обработчик доменных событий для production-service.
package kafka

import (
	"context"
	"log/slog"

	sharedkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/usecase"
)

const consumerName = "production-service"

type Handler struct {
	production *usecase.ProductionUsecase
	logger     *slog.Logger
	metrics    *observability.Metrics
}

func NewHandler(production *usecase.ProductionUsecase, logger *slog.Logger, metrics *observability.Metrics) *Handler {
	return &Handler{production: production, logger: logger, metrics: metrics}
}

func (h *Handler) Handle(ctx context.Context, event sharedkafka.Event) error {
	if event.EventType != sharedkafka.EventPaymentSucceeded {
		return sharedkafka.ErrSkip
	}

	task, err := h.production.StartFromPayment(ctx, event, consumerName)
	if err != nil {
		if usecase.IsDuplicateStart(err) {
			return sharedkafka.ErrAlreadyProcessed
		}
		return err
	}

	h.metrics.ProductionTasks.WithLabelValues(domain.StatusStarted).Inc()
	h.logger.InfoContext(ctx, "production task started", "task_id", task.ID)
	return nil
}
