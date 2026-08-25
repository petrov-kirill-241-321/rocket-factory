package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	kafkago "github.com/segmentio/kafka-go"
)

func testConsumer(handler Handler, maxAttempts int) *Consumer {
	cfg := ConsumerConfig{
		ConsumerName: "test-consumer",
		GroupID:      "test-consumer",
		MaxAttempts:  maxAttempts,
		RetryBackoff: time.Millisecond,
	}
	cfg.applyDefaults()

	return &Consumer{
		handler: handler,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: observability.NewMetrics("test"),
		cfg:     cfg,
	}
}

// Транзиентная ошибка (недоступность БД, взаимная блокировка) должна
// повторяться: раньше сообщение при любой ошибке просто терялось.
func TestHandleWithRetryRepeatsTransientErrors(t *testing.T) {
	attempts := 0
	handler := HandlerFunc(func(context.Context, Event) error {
		attempts++
		if attempts < 3 {
			return errors.New("временная недоступность базы данных")
		}
		return nil
	})

	consumer := testConsumer(handler, 5)

	err := consumer.handleWithRetry(context.Background(), Event{EventID: "e1"}, consumer.logger)
	if err != nil {
		t.Fatalf("ожидался успех после повторов, получено: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("попыток = %d, ожидалось 3", attempts)
	}
}

func TestHandleWithRetryStopsAtMaxAttempts(t *testing.T) {
	attempts := 0
	handler := HandlerFunc(func(context.Context, Event) error {
		attempts++
		return errors.New("постоянная недоступность")
	})

	consumer := testConsumer(handler, 4)

	err := consumer.handleWithRetry(context.Background(), Event{EventID: "e1"}, consumer.logger)
	if err == nil {
		t.Fatal("ожидалась ошибка после исчерпания попыток")
	}
	if attempts != 4 {
		t.Fatalf("попыток = %d, ожидалось 4", attempts)
	}
}

// Неисправимые ошибки и ошибки с известной семантикой повторять бессмысленно.
func TestHandleWithRetryDoesNotRepeatTerminalOutcomes(t *testing.T) {
	cases := map[string]error{
		"пропуск":            ErrSkip,
		"уже обработано":     ErrAlreadyProcessed,
		"устаревшее событие": ErrStale,
		"неисправимая":       Permanent(errors.New("битый payload")),
	}

	for name, outcome := range cases {
		t.Run(name, func(t *testing.T) {
			attempts := 0
			handler := HandlerFunc(func(context.Context, Event) error {
				attempts++
				return outcome
			})

			consumer := testConsumer(handler, 5)
			err := consumer.handleWithRetry(context.Background(), Event{EventID: "e1"}, consumer.logger)

			if attempts != 1 {
				t.Fatalf("попыток = %d, ожидалась 1", attempts)
			}
			if !errors.Is(err, outcome) {
				t.Fatalf("err = %v, ожидалось %v", err, outcome)
			}
		})
	}
}

func TestHandleWithRetryStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	handler := HandlerFunc(func(context.Context, Event) error {
		attempts++
		cancel()
		return errors.New("временная ошибка")
	})

	consumer := testConsumer(handler, 10)
	_ = consumer.handleWithRetry(ctx, Event{EventID: "e1"}, consumer.logger)

	if attempts != 1 {
		t.Fatalf("попыток = %d, ожидалась 1: после отмены контекста повторы прекращаются", attempts)
	}
}

func TestPermanentErrorUnwraps(t *testing.T) {
	cause := errors.New("исходная причина")
	wrapped := Permanent(cause)

	if !isPermanent(wrapped) {
		t.Fatal("ошибка должна распознаваться как неисправимая")
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("исходная ошибка должна быть доступна через errors.Is")
	}
	if isPermanent(cause) {
		t.Fatal("обычная ошибка не должна считаться неисправимой")
	}
}

func TestDecodeEventValidatesEnvelope(t *testing.T) {
	valid := NewEvent(EventOrderCreated, "order-1", "user-1", map[string]any{"items": []any{}})

	message, err := buildMessage(context.Background(), "order.events", valid)
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}

	decoded, err := DecodeEvent(message)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	if decoded.EventID != valid.EventID || decoded.EventType != EventOrderCreated {
		t.Fatalf("конверт декодирован неверно: %+v", decoded)
	}

	// Ключ сообщения — order_id: события одного заказа попадают в одну
	// партицию и сохраняют порядок.
	if string(message.Key) != "order-1" {
		t.Fatalf("ключ сообщения = %q, ожидался order-1", message.Key)
	}
}

func TestDecodeEventRejectsIncompleteEnvelope(t *testing.T) {
	payloads := []string{
		`{"event_id":"","event_type":"order_created","order_id":"o","user_id":"u"}`,
		`{"event_id":"e","event_type":"","order_id":"o","user_id":"u"}`,
		`{"event_id":"e","event_type":"order_created","order_id":"","user_id":"u"}`,
		`{"event_id":"e","event_type":"order_created","order_id":"o","user_id":""}`,
		`не json`,
	}

	for _, payload := range payloads {
		if _, err := DecodeEvent(kafkago.Message{Value: []byte(payload)}); err == nil {
			t.Fatalf("конверт %q должен быть отклонён", payload)
		}
	}
}
