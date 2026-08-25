package usecase

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/repository"
)

var ErrTaskNotFound = repository.ErrTaskNotFound

type ProductionUsecase struct {
	tasks           repository.Repository
	logger          *slog.Logger
	metrics         *observability.Metrics
	productionTopic string
	duration        time.Duration
}

func NewProductionUsecase(
	tasks repository.Repository,
	logger *slog.Logger,
	metrics *observability.Metrics,
	productionTopic string,
	duration time.Duration,
) *ProductionUsecase {
	return &ProductionUsecase{
		tasks:           tasks,
		logger:          logger,
		metrics:         metrics,
		productionTopic: productionTopic,
		duration:        duration,
	}
}

// StartFromPayment создаёт производственную задачу по факту успешной оплаты.
//
// Задача только фиксируется в БД; завершение выполняет фоновый Reconciler.
// Раньше здесь запускалась горутина с time.Sleep: при перезапуске сервиса
// в течение этого сна задача навсегда оставалась в статусе started,
// а заказ — в production_started.
func (u *ProductionUsecase) StartFromPayment(
	ctx context.Context,
	event kafka.Event,
	consumerName string,
) (domain.Task, error) {
	now := time.Now().UTC()
	task := domain.Task{
		ID:        uuid.NewString(),
		OrderID:   event.OrderID,
		UserID:    event.UserID,
		Status:    domain.StatusStarted,
		StartedAt: &now,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return u.tasks.CreateStarted(ctx, repository.CreateStartedParams{
		Task:         task,
		EventID:      event.EventID,
		EventType:    event.EventType,
		ConsumerName: consumerName,
		OutboxTopic:  u.productionTopic,
		BuildEvent: func(saved domain.Task) kafka.Event {
			return kafka.NewEvent(kafka.EventProductionStarted, saved.OrderID, saved.UserID, map[string]any{
				"production_task_id": saved.ID,
				"expected_duration":  u.duration.String(),
			})
		},
	})
}

func (u *ProductionUsecase) Get(ctx context.Context, taskID string) (domain.Task, error) {
	return u.tasks.GetByID(ctx, taskID)
}

// Complete завершает конкретную задачу. Идемпотентна: повторный вызов
// не публикует второе событие.
func (u *ProductionUsecase) Complete(ctx context.Context, taskID string) (domain.Task, bool, error) {
	return u.tasks.Complete(ctx, repository.CompleteParams{
		TaskID:      taskID,
		OutboxTopic: u.productionTopic,
		BuildEvent: func(saved domain.Task) kafka.Event {
			return kafka.NewEvent(kafka.EventProductionCompleted, saved.OrderID, saved.UserID, map[string]any{
				"production_task_id": saved.ID,
			})
		},
	})
}

// CompleteReady завершает все задачи, у которых истекло время производства.
func (u *ProductionUsecase) CompleteReady(ctx context.Context, limit int) (int, error) {
	cutoff := time.Now().UTC().Add(-u.duration)

	tasks, err := u.tasks.ListStartedBefore(ctx, cutoff, limit)
	if err != nil {
		return 0, err
	}

	completed := 0
	for _, task := range tasks {
		if ctx.Err() != nil {
			return completed, nil
		}

		_, changed, err := u.Complete(ctx, task.ID)
		if err != nil {
			if errors.Is(err, repository.ErrTaskNotFound) {
				continue
			}
			// Одна проблемная задача не должна останавливать обработку остальных.
			u.logger.ErrorContext(ctx, "complete production task",
				"task_id", task.ID, "order_id", task.OrderID, "error", err)
			continue
		}
		if changed {
			completed++
			u.metrics.ProductionTasks.WithLabelValues(domain.StatusCompleted).Inc()
		}
	}
	return completed, nil
}

// Reconciler периодически завершает готовые задачи.
//
// Это же и механизм восстановления: после перезапуска сервиса задачи,
// «зависшие» в статусе started, будут завершены при первом же проходе.
type Reconciler struct {
	production *ProductionUsecase
	logger     *slog.Logger
	interval   time.Duration
	batchSize  int
}

func NewReconciler(production *ProductionUsecase, logger *slog.Logger, interval time.Duration) *Reconciler {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Reconciler{production: production, logger: logger, interval: interval, batchSize: 50}
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		completed, err := r.production.CompleteReady(ctx, r.batchSize)
		if err != nil && ctx.Err() == nil {
			r.logger.ErrorContext(ctx, "complete ready production tasks", "error", err)
		}
		if completed > 0 {
			r.logger.InfoContext(ctx, "production tasks completed", "count", completed)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// IsDuplicateStart сообщает, что задача для заказа уже создана.
func IsDuplicateStart(err error) bool {
	return errors.Is(err, repository.ErrDuplicateEvent) || errors.Is(err, repository.ErrTaskExists)
}
