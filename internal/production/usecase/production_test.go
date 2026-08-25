package usecase

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/repository"
)

func newUsecase(repo repository.Repository, duration time.Duration) *ProductionUsecase {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewProductionUsecase(repo, logger, observability.NewMetrics("production-service-test"), "production.events", duration)
}

func TestStartFromPaymentCreatesTask(t *testing.T) {
	repo := newMemoryRepository()
	uc := newUsecase(repo, time.Millisecond)

	task, err := uc.StartFromPayment(context.Background(), kafka.Event{
		EventID:   "event-1",
		EventType: kafka.EventPaymentSucceeded,
		OrderID:   "order-1",
		UserID:    "user-1",
	}, "production-service")
	if err != nil {
		t.Fatalf("start production: %v", err)
	}

	if task.Status != domain.StatusStarted {
		t.Fatalf("статус = %s, ожидался %s", task.Status, domain.StatusStarted)
	}

	events := repo.snapshot()
	if len(events) != 1 || events[0].EventType != kafka.EventProductionStarted {
		t.Fatalf("ожидалось одно событие production_started, получено %+v", events)
	}
	// Регресс: событие должно нести идентификаторы сохранённой задачи,
	// а не сгенерированные наугад.
	if events[0].OrderID != "order-1" || events[0].UserID != "user-1" {
		t.Fatalf("в событии неверные идентификаторы: order=%s user=%s", events[0].OrderID, events[0].UserID)
	}
}

// Завершение выполняет фоновый цикл, а не горутина со Sleep: благодаря этому
// задачи, «зависшие» после перезапуска сервиса, всё равно доводятся до конца.
func TestCompleteReadyFinishesExpiredTasks(t *testing.T) {
	repo := newMemoryRepository()
	uc := newUsecase(repo, time.Millisecond)
	ctx := context.Background()

	if _, err := uc.StartFromPayment(ctx, kafka.Event{
		EventID: "event-1", OrderID: "order-1", UserID: "user-1",
	}, "production-service"); err != nil {
		t.Fatalf("start production: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	completed, err := uc.CompleteReady(ctx, 10)
	if err != nil {
		t.Fatalf("complete ready: %v", err)
	}
	if completed != 1 {
		t.Fatalf("завершено задач: %d, ожидалась 1", completed)
	}

	events := repo.snapshot()
	if len(events) != 2 || events[1].EventType != kafka.EventProductionCompleted {
		t.Fatalf("ожидалось событие production_completed, получено %+v", events)
	}
	if events[1].OrderID != "order-1" {
		t.Fatalf("order_id в событии завершения = %s, ожидался order-1", events[1].OrderID)
	}
}

// Задача, у которой время производства ещё не истекло, завершаться не должна.
func TestCompleteReadySkipsUnfinishedTasks(t *testing.T) {
	repo := newMemoryRepository()
	uc := newUsecase(repo, time.Hour)

	if _, err := uc.StartFromPayment(context.Background(), kafka.Event{
		EventID: "event-1", OrderID: "order-1", UserID: "user-1",
	}, "production-service"); err != nil {
		t.Fatalf("start production: %v", err)
	}

	completed, err := uc.CompleteReady(context.Background(), 10)
	if err != nil {
		t.Fatalf("complete ready: %v", err)
	}
	if completed != 0 {
		t.Fatalf("завершено задач: %d, ожидалось 0", completed)
	}
}

// Повторное завершение не должно публиковать второе событие.
func TestCompleteIsIdempotent(t *testing.T) {
	repo := newMemoryRepository()
	uc := newUsecase(repo, time.Millisecond)
	ctx := context.Background()

	task, err := uc.StartFromPayment(ctx, kafka.Event{
		EventID: "event-1", OrderID: "order-1", UserID: "user-1",
	}, "production-service")
	if err != nil {
		t.Fatalf("start production: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, changed, err := uc.Complete(ctx, task.ID)
		if err != nil {
			t.Fatalf("complete #%d: %v", i, err)
		}
		if i > 0 && changed {
			t.Fatalf("повторный вызов #%d изменил задачу", i)
		}
	}

	if events := repo.snapshot(); len(events) != 2 {
		t.Fatalf("событий = %d, ожидалось 2 (started + completed)", len(events))
	}
}

func TestDuplicateStartIsDetected(t *testing.T) {
	repo := newMemoryRepository()
	uc := newUsecase(repo, time.Millisecond)
	ctx := context.Background()

	event := kafka.Event{EventID: "event-1", OrderID: "order-1", UserID: "user-1"}

	if _, err := uc.StartFromPayment(ctx, event, "production-service"); err != nil {
		t.Fatalf("первый запуск: %v", err)
	}

	_, err := uc.StartFromPayment(ctx, event, "production-service")
	if !IsDuplicateStart(err) {
		t.Fatalf("err = %v, ожидался признак повторного запуска", err)
	}
}

// --- тестовый двойник ---

type memoryRepository struct {
	mu        sync.Mutex
	tasks     map[string]domain.Task
	byOrder   map[string]string
	processed map[string]bool
	events    []kafka.Event
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		tasks:     map[string]domain.Task{},
		byOrder:   map[string]string{},
		processed: map[string]bool{},
	}
}

func (r *memoryRepository) CreateStarted(_ context.Context, params repository.CreateStartedParams) (domain.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if params.EventID != "" {
		if r.processed[params.EventID] {
			return domain.Task{}, repository.ErrDuplicateEvent
		}
		r.processed[params.EventID] = true
	}
	if _, exists := r.byOrder[params.Task.OrderID]; exists {
		return domain.Task{}, repository.ErrTaskExists
	}

	task := params.Task
	r.tasks[task.ID] = task
	r.byOrder[task.OrderID] = task.ID
	r.events = append(r.events, params.BuildEvent(task))
	return task, nil
}

func (r *memoryRepository) Complete(_ context.Context, params repository.CompleteParams) (domain.Task, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	task, ok := r.tasks[params.TaskID]
	if !ok {
		return domain.Task{}, false, repository.ErrTaskNotFound
	}
	if task.Status != domain.StatusStarted {
		return task, false, nil
	}

	now := time.Now().UTC()
	task.Status = domain.StatusCompleted
	task.CompletedAt = &now
	task.UpdatedAt = now
	r.tasks[params.TaskID] = task
	r.events = append(r.events, params.BuildEvent(task))
	return task, true, nil
}

func (r *memoryRepository) GetByID(_ context.Context, taskID string) (domain.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	task, ok := r.tasks[taskID]
	if !ok {
		return domain.Task{}, repository.ErrTaskNotFound
	}
	return task, nil
}

func (r *memoryRepository) ListStartedBefore(_ context.Context, cutoff time.Time, limit int) ([]domain.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]domain.Task, 0)
	for _, task := range r.tasks {
		if task.Status != domain.StatusStarted || task.StartedAt == nil {
			continue
		}
		if task.StartedAt.After(cutoff) {
			continue
		}
		out = append(out, task)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *memoryRepository) snapshot() []kafka.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]kafka.Event, len(r.events))
	copy(out, r.events)
	return out
}
