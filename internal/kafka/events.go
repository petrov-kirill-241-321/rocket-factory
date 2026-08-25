package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	EventOrderCreated        = "order_created"
	EventInventoryReserved   = "inventory_reserved"
	EventInventoryFailed     = "inventory_failed"
	EventInventoryReleased   = "inventory_released"
	EventInventoryCommitted  = "inventory_committed"
	EventPaymentSucceeded    = "payment_succeeded"
	EventPaymentFailed       = "payment_failed"
	EventProductionStarted   = "production_started"
	EventProductionCompleted = "production_completed"
	EventConsumerFailed      = "consumer_failed"
)

// Event — единый конверт доменного события.
type Event struct {
	EventID   string         `json:"event_id"`
	EventType string         `json:"event_type"`
	OrderID   string         `json:"order_id"`
	UserID    string         `json:"user_id"`
	CreatedAt time.Time      `json:"created_at"`
	Payload   map[string]any `json:"payload"`
}

func NewEvent(eventType, orderID, userID string, payload map[string]any) Event {
	if payload == nil {
		payload = map[string]any{}
	}
	return Event{
		EventID:   uuid.NewString(),
		EventType: eventType,
		OrderID:   orderID,
		UserID:    userID,
		CreatedAt: time.Now().UTC(),
		Payload:   payload,
	}
}

// TraceContext сериализует текущий trace context в map, пригодный для хранения в outbox.
// Без этого трейс рвётся: событие публикуется фоновой горутиной спустя время после
// коммита транзакции, в другом контексте.
func TraceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// ContextFromTrace восстанавливает trace context, сохранённый в outbox.
func ContextFromTrace(ctx context.Context, traceContext map[string]string) context.Context {
	if len(traceContext) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(traceContext))
}

// messageCarrier переносит trace context через заголовки Kafka-сообщения.
type messageCarrier struct {
	headers *[]kafkago.Header
}

func (c messageCarrier) Get(key string) string {
	for _, header := range *c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c messageCarrier) Set(key, value string) {
	for i, header := range *c.headers {
		if header.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafkago.Header{Key: key, Value: []byte(value)})
}

func (c messageCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

// DynamicProducer публикует в топик, указанный в момент вызова.
type DynamicProducer struct {
	writer *kafkago.Writer
}

func NewDynamicProducer(brokers []string) *DynamicProducer {
	return &DynamicProducer{writer: newWriter(brokers, "")}
}

func (p *DynamicProducer) PublishTo(ctx context.Context, topic string, event Event) error {
	message, err := buildMessage(ctx, topic, event)
	if err != nil {
		return err
	}
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish kafka event %s to %s: %w", event.EventType, topic, err)
	}
	return nil
}

func (p *DynamicProducer) Close() error {
	return p.writer.Close()
}

func newWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		MaxAttempts:  5,
		WriteTimeout: 10 * time.Second,
		Async:        false,
	}
}

func buildMessage(ctx context.Context, topic string, event Event) (kafkago.Message, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("marshal kafka event: %w", err)
	}

	headers := []kafkago.Header{
		{Key: "event_id", Value: []byte(event.EventID)},
		{Key: "event_type", Value: []byte(event.EventType)},
	}
	otel.GetTextMapPropagator().Inject(ctx, messageCarrier{headers: &headers})

	return kafkago.Message{
		Topic:   topic,
		Key:     []byte(event.OrderID),
		Value:   data,
		Time:    event.CreatedAt,
		Headers: headers,
	}, nil
}

// DecodeEvent разбирает и валидирует конверт события.
func DecodeEvent(message kafkago.Message) (Event, error) {
	var event Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return Event{}, fmt.Errorf("decode kafka event: %w", err)
	}
	if event.EventID == "" || event.EventType == "" || event.OrderID == "" || event.UserID == "" {
		return Event{}, fmt.Errorf("invalid kafka event envelope")
	}
	if event.Payload == nil {
		event.Payload = map[string]any{}
	}
	return event, nil
}

// ContextFromMessage восстанавливает trace context из заголовков сообщения.
func ContextFromMessage(ctx context.Context, message kafkago.Message) context.Context {
	headers := message.Headers
	return otel.GetTextMapPropagator().Extract(ctx, messageCarrier{headers: &headers})
}
