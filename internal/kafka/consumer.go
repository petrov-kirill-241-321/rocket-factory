package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Семантика ошибок обработчика. От неё зависит, коммитить ли offset,
// ретраить ли сообщение и отправлять ли его в DLQ.
var (
	// ErrSkip — событие не адресовано этому консьюмеру. Коммитим, ничего не делаем.
	ErrSkip = errors.New("event skipped by consumer")
	// ErrAlreadyProcessed — событие уже обработано. Коммитим (идемпотентный no-op).
	ErrAlreadyProcessed = errors.New("event already processed")
	// ErrStale — событие устарело: агрегат уже в более позднем состоянии.
	// Возникает при доставке событий не по порядку. Коммитим, повтор бессмыслен.
	ErrStale = errors.New("event is stale for current aggregate state")
)

// permanentError помечает ошибку как неисправимую повтором: битые данные,
// нарушение инварианта. Такое сообщение уходит в DLQ немедленно.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent помечает ошибку как не подлежащую повтору.
func Permanent(err error) error { return permanentError{err: err} }

func isPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

// Handler обрабатывает одно доменное событие.
type Handler interface {
	Handle(ctx context.Context, event Event) error
}

// HandlerFunc адаптирует функцию к интерфейсу Handler.
type HandlerFunc func(ctx context.Context, event Event) error

func (f HandlerFunc) Handle(ctx context.Context, event Event) error { return f(ctx, event) }

// Deduplicator — быстрый предварительный фильтр повторов (Redis).
// Источником истины остаётся таблица processed_events в бизнес-транзакции,
// поэтому недоступность дедупликатора не блокирует обработку.
type Deduplicator interface {
	IsProcessed(ctx context.Context, consumerName, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, consumerName, eventID string) error
}

// ConsumerConfig описывает параметры запуска консьюмера.
type ConsumerConfig struct {
	Brokers      []string
	Topics       []string
	GroupID      string
	ConsumerName string
	DLQTopic     string
	MaxAttempts  int
	RetryBackoff time.Duration
}

func (c *ConsumerConfig) applyDefaults() {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	if c.ConsumerName == "" {
		c.ConsumerName = c.GroupID
	}
}

// Consumer читает события из Kafka с семантикой at-least-once:
// offset коммитится только после успешной обработки. Транзиентные ошибки
// повторяются с экспоненциальным backoff, неисправимые уходят в DLQ.
type Consumer struct {
	reader  *kafkago.Reader
	handler Handler
	deduper Deduplicator
	dlq     *DynamicProducer
	logger  *slog.Logger
	metrics *observability.Metrics
	tracer  trace.Tracer
	cfg     ConsumerConfig
}

func NewConsumer(
	cfg ConsumerConfig,
	handler Handler,
	deduper Deduplicator,
	dlq *DynamicProducer,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *Consumer {
	cfg.applyDefaults()
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     cfg.Brokers,
			GroupTopics: cfg.Topics,
			GroupID:     cfg.GroupID,
			StartOffset: kafkago.FirstOffset,
			MaxWait:     time.Second,
		}),
		handler: handler,
		deduper: deduper,
		dlq:     dlq,
		logger:  logger,
		metrics: metrics,
		tracer:  otel.Tracer(cfg.ConsumerName + "/kafka"),
		cfg:     cfg,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.metrics.KafkaErrorCount.WithLabelValues(c.cfg.ConsumerName, "fetch").Inc()
			c.logger.ErrorContext(ctx, "fetch kafka message", "error", err)
			if !sleepCtx(ctx, c.cfg.RetryBackoff) {
				return nil
			}
			continue
		}

		c.processMessage(ctx, message)
	}
}

func (c *Consumer) processMessage(ctx context.Context, message kafkago.Message) {
	msgCtx := ContextFromMessage(ctx, message)

	event, err := DecodeEvent(message)
	if err != nil {
		c.metrics.KafkaErrorCount.WithLabelValues(c.cfg.ConsumerName, "decode").Inc()
		c.logger.ErrorContext(msgCtx, "decode kafka event",
			"topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
		c.sendToDLQ(msgCtx, "decode_error", err, nil, message.Value)
		c.commit(ctx, message)
		return
	}

	spanCtx, span := c.tracer.Start(msgCtx, "consume "+event.EventType,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", message.Topic),
			attribute.String("messaging.consumer.group.name", c.cfg.GroupID),
			attribute.String("event.id", event.EventID),
			attribute.String("event.type", event.EventType),
			attribute.String("order.id", event.OrderID),
		))
	defer span.End()

	logger := c.logger.With(
		"event_id", event.EventID,
		"event_type", event.EventType,
		"order_id", event.OrderID,
		"topic", message.Topic,
	)

	if processed, dedupErr := c.deduper.IsProcessed(spanCtx, c.cfg.ConsumerName, event.EventID); dedupErr != nil {
		// Redis недоступен — не блокируем обработку, processed_events защитит от повтора.
		logger.WarnContext(spanCtx, "event dedup cache unavailable", "error", dedupErr)
	} else if processed {
		c.metrics.KafkaSkippedCount.WithLabelValues(c.cfg.ConsumerName, event.EventType, "duplicate").Inc()
		span.SetAttributes(attribute.Bool("event.duplicate", true))
		c.commit(ctx, message)
		return
	}

	started := time.Now()
	err = c.handleWithRetry(spanCtx, event, logger)
	c.metrics.KafkaProcessingDuration.
		WithLabelValues(c.cfg.ConsumerName, event.EventType).
		Observe(time.Since(started).Seconds())

	switch {
	case err == nil:
		c.metrics.KafkaProcessedCount.WithLabelValues(c.cfg.ConsumerName, event.EventType).Inc()
		c.markProcessed(spanCtx, event, logger)

	case errors.Is(err, ErrSkip):
		c.metrics.KafkaSkippedCount.WithLabelValues(c.cfg.ConsumerName, event.EventType, "not_subscribed").Inc()

	case errors.Is(err, ErrAlreadyProcessed):
		c.metrics.KafkaSkippedCount.WithLabelValues(c.cfg.ConsumerName, event.EventType, "duplicate").Inc()
		c.markProcessed(spanCtx, event, logger)

	case errors.Is(err, ErrStale):
		// Событие пришло не по порядку и уже неактуально. Это штатная ситуация
		// для at-least-once: агрегат уже в более позднем состоянии.
		c.metrics.KafkaSkippedCount.WithLabelValues(c.cfg.ConsumerName, event.EventType, "stale").Inc()
		logger.InfoContext(spanCtx, "stale event skipped")
		c.markProcessed(spanCtx, event, logger)

	default:
		// Ретраи исчерпаны либо ошибка неисправима — в DLQ, чтобы не блокировать партицию.
		c.metrics.KafkaErrorCount.WithLabelValues(c.cfg.ConsumerName, event.EventType).Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorContext(spanCtx, "handle kafka event failed, routing to dlq", "error", err)
		c.sendToDLQ(spanCtx, "handler_error", err, &event, message.Value)
	}

	c.commit(ctx, message)
}

// handleWithRetry повторяет обработку при транзиентных ошибках (недоступность БД,
// deadlock). Ошибки с известной семантикой и permanent-ошибки возвращаются сразу.
func (c *Consumer) handleWithRetry(ctx context.Context, event Event, logger *slog.Logger) error {
	backoff := c.cfg.RetryBackoff
	var lastErr error

	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		lastErr = c.handler.Handle(ctx, event)
		if lastErr == nil ||
			errors.Is(lastErr, ErrSkip) ||
			errors.Is(lastErr, ErrAlreadyProcessed) ||
			errors.Is(lastErr, ErrStale) ||
			isPermanent(lastErr) {
			return lastErr
		}

		if attempt == c.cfg.MaxAttempts {
			break
		}

		logger.WarnContext(ctx, "retrying kafka event",
			"attempt", attempt, "max_attempts", c.cfg.MaxAttempts, "backoff", backoff.String(), "error", lastErr)
		if !sleepCtx(ctx, backoff) {
			return lastErr
		}
		backoff *= 2
	}

	return fmt.Errorf("handle event after %d attempts: %w", c.cfg.MaxAttempts, lastErr)
}

func (c *Consumer) markProcessed(ctx context.Context, event Event, logger *slog.Logger) {
	if err := c.deduper.MarkProcessed(ctx, c.cfg.ConsumerName, event.EventID); err != nil {
		logger.WarnContext(ctx, "mark event processed in dedup cache", "error", err)
	}
}

func (c *Consumer) commit(ctx context.Context, message kafkago.Message) {
	// Коммит должен пройти даже при отменённом контексте, иначе после shutdown
	// сообщение будет обработано повторно.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := c.reader.CommitMessages(commitCtx, message); err != nil {
		c.metrics.KafkaErrorCount.WithLabelValues(c.cfg.ConsumerName, "commit").Inc()
		c.logger.ErrorContext(commitCtx, "commit kafka message",
			"topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
	}
}

func (c *Consumer) sendToDLQ(ctx context.Context, reason string, handlerErr error, event *Event, raw []byte) {
	if c.dlq == nil || c.cfg.DLQTopic == "" {
		return
	}

	orderID := uuidZero
	userID := uuidZero
	payload := map[string]any{
		"consumer": c.cfg.ConsumerName,
		"reason":   reason,
		"error":    handlerErr.Error(),
		"raw":      string(raw),
	}
	if event != nil {
		orderID = event.OrderID
		userID = event.UserID
		payload["source_event_id"] = event.EventID
		payload["source_event_type"] = event.EventType
	}

	dlqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := c.dlq.PublishTo(dlqCtx, c.cfg.DLQTopic, NewEvent(EventConsumerFailed, orderID, userID, payload)); err != nil {
		c.logger.ErrorContext(dlqCtx, "publish dlq event", "reason", reason, "error", err)
		return
	}
	c.metrics.KafkaDLQCount.WithLabelValues(c.cfg.ConsumerName, reason).Inc()
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

// uuidZero используется вместо строки "unknown": order_id/user_id объявлены как uuid,
// и потребитель DLQ, пишущий их в БД, не должен падать на несовместимом значении.
const uuidZero = "00000000-0000-0000-0000-000000000000"

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
