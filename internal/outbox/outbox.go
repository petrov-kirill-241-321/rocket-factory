// Package outbox реализует transactional outbox: событие сохраняется в той же
// транзакции, что и изменение состояния, а публикуется отдельным процессом.
// Это гарантирует, что событие не потеряется при падении сервиса между
// коммитом в БД и отправкой в брокер.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
)

// maxAttempts согласован с частичным индексом outbox_events_unpublished_idx.
const maxAttempts = 10

// Record — строка outbox, готовая к публикации.
type Record struct {
	ID           string
	Topic        string
	Payload      []byte
	TraceContext map[string]string
}

// Stats описывает состояние очереди публикации.
type Stats struct {
	Pending           int64
	OldestPendingAge  time.Duration
	ExhaustedAttempts int64
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) Store {
	return Store{db: db}
}

// InsertTx записывает событие в outbox внутри переданной транзакции.
// Вместе с событием сохраняется trace context: публикация произойдёт позже,
// в другой горутине, и без него трейс заказа рвётся на каждой границе сервиса.
func InsertTx(ctx context.Context, tx pgx.Tx, topic string, event kafka.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal outbox event: %w", err)
	}

	// pgx кодирует map в jsonb самостоятельно; nil сохраняется как NULL.
	var traceContext any
	if carrier := kafka.TraceContext(ctx); len(carrier) > 0 {
		traceContext = map[string]string(carrier)
	}

	_, err = tx.Exec(ctx, `
		insert into outbox_events
			(id, topic, event_id, event_type, order_id, user_id, payload, trace_context, created_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		on conflict (event_id) do nothing
	`, uuid.NewString(), topic, event.EventID, event.EventType,
		event.OrderID, event.UserID, payload, traceContext, event.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return fmt.Errorf("insert outbox event (%s): %w", pgErr.Code, err)
		}
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// FetchBatch забирает пачку неопубликованных событий под блокировку.
// skip locked позволяет нескольким публикаторам работать параллельно,
// не блокируя друг друга и не выдавая одну строку дважды.
func (s Store) FetchBatch(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.db.Query(ctx, `
		with picked as (
			select id
			from outbox_events
			where published_at is null
			  and attempts < $2
			  and (locked_until is null or locked_until < now())
			order by created_at
			limit $1
			for update skip locked
		)
		update outbox_events o
		set locked_until = now() + interval '1 minute'
		from picked
		where o.id = picked.id
		returning o.id, o.topic, o.payload, o.trace_context
	`, limit, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox events: %w", err)
	}
	defer rows.Close()

	out := make([]Record, 0, limit)
	for rows.Next() {
		var (
			item     Record
			rawTrace []byte
		)
		if err := rows.Scan(&item.ID, &item.Topic, &item.Payload, &rawTrace); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		if len(rawTrace) > 0 {
			if err := json.Unmarshal(rawTrace, &item.TraceContext); err != nil {
				// Потеря trace context не должна блокировать доставку события.
				item.TraceContext = nil
			}
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return out, nil
}

func (s Store) MarkPublished(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `
		update outbox_events
		set published_at = now(), last_error = null, locked_until = null
		where id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

func (s Store) MarkFailed(ctx context.Context, id string, publishErr error) error {
	_, err := s.db.Exec(ctx, `
		update outbox_events
		set attempts = attempts + 1, last_error = $2, locked_until = null
		where id = $1
	`, id, publishErr.Error())
	if err != nil {
		return fmt.Errorf("mark outbox event failed: %w", err)
	}
	return nil
}

// Stats нужен для мониторинга: рост очереди или возраста самого старого
// события — первый признак того, что доставка событий встала.
func (s Store) Stats(ctx context.Context) (Stats, error) {
	var (
		stats     Stats
		oldestAge *float64
	)
	err := s.db.QueryRow(ctx, `
		select
			count(*) filter (where published_at is null and attempts < $1),
			count(*) filter (where published_at is null and attempts >= $1),
			extract(epoch from (now() - min(created_at) filter (where published_at is null)))
		from outbox_events
	`, maxAttempts).Scan(&stats.Pending, &stats.ExhaustedAttempts, &oldestAge)
	if err != nil {
		return Stats{}, fmt.Errorf("collect outbox stats: %w", err)
	}
	if oldestAge != nil && *oldestAge > 0 {
		stats.OldestPendingAge = time.Duration(*oldestAge * float64(time.Second))
	}
	return stats, nil
}

// Cleanup удаляет успешно опубликованные события старше retention.
// Без него таблица растёт неограниченно.
func (s Store) Cleanup(ctx context.Context, retention time.Duration) (int64, error) {
	tag, err := s.db.Exec(ctx, `
		delete from outbox_events
		where published_at is not null
		  and published_at < now() - make_interval(secs => $1)
	`, retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("cleanup outbox events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Config задаёт параметры фонового публикатора.
type Config struct {
	Interval        time.Duration
	BatchSize       int
	CleanupInterval time.Duration
	Retention       time.Duration
}

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = time.Hour
	}
	if c.Retention <= 0 {
		c.Retention = 7 * 24 * time.Hour
	}
}

// Publisher периодически публикует накопленные события в Kafka.
type Publisher struct {
	store    Store
	producer *kafka.DynamicProducer
	logger   *slog.Logger
	metrics  *observability.Metrics
	cfg      Config
}

func NewPublisher(
	store Store,
	producer *kafka.DynamicProducer,
	logger *slog.Logger,
	metrics *observability.Metrics,
	cfg Config,
) *Publisher {
	cfg.applyDefaults()
	return &Publisher{store: store, producer: producer, logger: logger, metrics: metrics, cfg: cfg}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	cleanup := time.NewTicker(p.cfg.CleanupInterval)
	defer cleanup.Stop()

	for {
		p.flush(ctx)
		p.reportStats(ctx)

		select {
		case <-ctx.Done():
			return
		case <-cleanup.C:
			p.cleanup(ctx)
		case <-ticker.C:
		}
	}
}

func (p *Publisher) flush(ctx context.Context) {
	records, err := p.store.FetchBatch(ctx, p.cfg.BatchSize)
	if err != nil {
		if ctx.Err() == nil {
			p.logger.ErrorContext(ctx, "fetch outbox batch", "error", err)
		}
		return
	}

	for _, record := range records {
		var event kafka.Event
		if err := json.Unmarshal(record.Payload, &event); err != nil {
			// Повтор не поможет: событие в БД нечитаемо. Исчерпываем попытки,
			// чтобы строка перестала выбираться и была видна в метрике.
			p.logger.ErrorContext(ctx, "decode outbox event", "outbox_id", record.ID, "error", err)
			p.exhaust(ctx, record.ID, err)
			p.metrics.OutboxPublished.WithLabelValues(record.Topic, "invalid").Inc()
			continue
		}

		// Восстанавливаем trace context, сохранённый в момент создания события.
		publishCtx := kafka.ContextFromTrace(ctx, record.TraceContext)

		if err := p.producer.PublishTo(publishCtx, record.Topic, event); err != nil {
			if markErr := p.store.MarkFailed(ctx, record.ID, err); markErr != nil {
				p.logger.ErrorContext(ctx, "mark outbox event failed", "outbox_id", record.ID, "error", markErr)
			}
			p.metrics.OutboxPublished.WithLabelValues(record.Topic, "error").Inc()
			p.logger.ErrorContext(publishCtx, "publish outbox event",
				"outbox_id", record.ID, "topic", record.Topic,
				"event_id", event.EventID, "event_type", event.EventType, "error", err)
			continue
		}

		if err := p.store.MarkPublished(ctx, record.ID); err != nil {
			// Событие уже в Kafka. Повторная публикация возможна, но безопасна:
			// потребители дедуплицируют по event_id.
			p.logger.ErrorContext(ctx, "mark outbox event published", "outbox_id", record.ID, "error", err)
		}
		p.metrics.OutboxPublished.WithLabelValues(record.Topic, "success").Inc()
	}
}

func (p *Publisher) exhaust(ctx context.Context, id string, cause error) {
	for i := 0; i < maxAttempts; i++ {
		if err := p.store.MarkFailed(ctx, id, cause); err != nil {
			p.logger.ErrorContext(ctx, "exhaust outbox event", "outbox_id", id, "error", err)
			return
		}
	}
}

func (p *Publisher) reportStats(ctx context.Context) {
	stats, err := p.store.Stats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			p.logger.WarnContext(ctx, "collect outbox stats", "error", err)
		}
		return
	}
	p.metrics.OutboxPendingTotal.Set(float64(stats.Pending))
	p.metrics.OutboxOldestSeconds.Set(stats.OldestPendingAge.Seconds())

	if stats.ExhaustedAttempts > 0 {
		p.logger.WarnContext(ctx, "outbox events exhausted retry attempts", "count", stats.ExhaustedAttempts)
	}
}

func (p *Publisher) cleanup(ctx context.Context) {
	removed, err := p.store.Cleanup(ctx, p.cfg.Retention)
	if err != nil {
		if ctx.Err() == nil {
			p.logger.WarnContext(ctx, "cleanup outbox events", "error", err)
		}
		return
	}
	if removed > 0 {
		p.logger.InfoContext(ctx, "cleaned up published outbox events", "removed", removed)
	}
}
