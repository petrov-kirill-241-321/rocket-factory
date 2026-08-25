package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/domain"
)

var (
	ErrTaskNotFound   = errors.New("production task not found")
	ErrTaskExists     = errors.New("production task already exists")
	ErrDuplicateEvent = errors.New("event already processed")
)

const uniqueViolation = "23505"

// EventBuilder строит исходящее событие уже по сохранённой задаче.
//
// Так исключается ситуация, когда use case формирует событие «вслепую»
// со случайными order_id и user_id в расчёте на то, что репозиторий их
// потом перезапишет.
type EventBuilder func(task domain.Task) kafka.Event

type CreateStartedParams struct {
	Task         domain.Task
	EventID      string
	EventType    string
	ConsumerName string
	OutboxTopic  string
	BuildEvent   EventBuilder
}

type CompleteParams struct {
	TaskID      string
	OutboxTopic string
	BuildEvent  EventBuilder
}

type Repository interface {
	CreateStarted(ctx context.Context, params CreateStartedParams) (domain.Task, error)
	Complete(ctx context.Context, params CompleteParams) (domain.Task, bool, error)
	GetByID(ctx context.Context, taskID string) (domain.Task, error)
	ListStartedBefore(ctx context.Context, cutoff time.Time, limit int) ([]domain.Task, error)
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateStarted(ctx context.Context, params CreateStartedParams) (domain.Task, error) {
	task := params.Task

	err := r.inTx(ctx, func(tx pgx.Tx) error {
		if params.EventID != "" {
			_, err := tx.Exec(ctx, `
				insert into processed_events (event_id, event_type, consumer_name, processed_at)
				values ($1, $2, $3, now())
			`, params.EventID, params.EventType, params.ConsumerName)
			if err != nil {
				if isUniqueViolation(err) {
					return ErrDuplicateEvent
				}
				return fmt.Errorf("insert processed event: %w", err)
			}
		}

		_, err := tx.Exec(ctx, `
			insert into production_tasks
				(id, order_id, user_id, status, started_at, completed_at, created_at, updated_at)
			values ($1, $2, $3, $4, $5, $6, $7, $8)
		`, task.ID, task.OrderID, task.UserID, task.Status,
			task.StartedAt, task.CompletedAt, task.CreatedAt, task.UpdatedAt)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrTaskExists
			}
			return fmt.Errorf("insert production task: %w", err)
		}

		return outbox.InsertTx(ctx, tx, params.OutboxTopic, params.BuildEvent(task))
	})
	if err != nil {
		return domain.Task{}, err
	}
	return task, nil
}

// Complete завершает задачу. Второе возвращаемое значение сообщает, была ли
// задача действительно переведена в completed: повторный вызов для уже
// завершённой задачи не создаёт второго события.
func (r *PostgresRepository) Complete(ctx context.Context, params CompleteParams) (domain.Task, bool, error) {
	var (
		task    domain.Task
		changed bool
	)

	err := r.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		task, err = scanTask(tx.QueryRow(ctx, `
			select id, order_id, user_id, status, started_at, completed_at, created_at, updated_at
			from production_tasks
			where id = $1
			for update
		`, params.TaskID))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrTaskNotFound
			}
			return fmt.Errorf("lock production task: %w", err)
		}

		// Завершать можно только запущенную задачу. Блокировка строки делает
		// проверку устойчивой к параллельным вызовам.
		if task.Status != domain.StatusStarted {
			return nil
		}

		task, err = scanTask(tx.QueryRow(ctx, `
			update production_tasks
			set status = $2, completed_at = now(), updated_at = now()
			where id = $1
			returning id, order_id, user_id, status, started_at, completed_at, created_at, updated_at
		`, params.TaskID, domain.StatusCompleted))
		if err != nil {
			return fmt.Errorf("complete production task: %w", err)
		}
		changed = true

		return outbox.InsertTx(ctx, tx, params.OutboxTopic, params.BuildEvent(task))
	})
	if err != nil {
		return domain.Task{}, false, err
	}
	return task, changed, nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, taskID string) (domain.Task, error) {
	task, err := scanTask(r.db.QueryRow(ctx, `
		select id, order_id, user_id, status, started_at, completed_at, created_at, updated_at
		from production_tasks
		where id = $1
	`, taskID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Task{}, ErrTaskNotFound
		}
		return domain.Task{}, fmt.Errorf("get production task: %w", err)
	}
	return task, nil
}

// ListStartedBefore возвращает задачи, у которых истекло время производства.
// На этом запросе держится восстановление после перезапуска сервиса.
func (r *PostgresRepository) ListStartedBefore(ctx context.Context, cutoff time.Time, limit int) ([]domain.Task, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	rows, err := r.db.Query(ctx, `
		select id, order_id, user_id, status, started_at, completed_at, created_at, updated_at
		from production_tasks
		where status = $1 and started_at <= $2
		order by started_at
		limit $3
	`, domain.StatusStarted, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("list started production tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]domain.Task, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan production task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate production tasks: %w", err)
	}
	return tasks, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTask(row scanner) (domain.Task, error) {
	var task domain.Task
	err := row.Scan(
		&task.ID, &task.OrderID, &task.UserID, &task.Status,
		&task.StartedAt, &task.CompletedAt, &task.CreatedAt, &task.UpdatedAt,
	)
	return task, err
}

func (r *PostgresRepository) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}
