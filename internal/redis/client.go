package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func Connect(ctx context.Context, addr, password string, db int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:            addr,
		Password:        password,
		DB:              db,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    2 * time.Second,
		MaxRetries:      3,
		MinRetryBackoff: 20 * time.Millisecond,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}

// ErrUnavailable сигнализирует, что Redis недоступен и вызывающий код должен
// решить, продолжать ли работу. Источником истины для идемпотентности остаётся
// уникальный индекс в PostgreSQL, поэтому падение Redis не обязано ронять запрос.
var ErrUnavailable = errors.New("redis is unavailable")

// IdempotencyStore реализует «claim → работа → подтверждение/освобождение».
//
// Ключ выставляется до записи в БД, поэтому при ошибке его обязательно нужно
// снять: иначе повторный запрос клиента получит 404 вместо повторной попытки.
type IdempotencyStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewIdempotencyStore(client *redis.Client, ttl time.Duration) IdempotencyStore {
	return IdempotencyStore{client: client, ttl: ttl}
}

// Claim пытается захватить ключ. Возвращает false, если операция с таким ключом
// уже выполняется или выполнена.
func (s IdempotencyStore) Claim(ctx context.Context, key string) (bool, error) {
	ok, err := s.client.SetNX(ctx, key, claimInProgress, s.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("%w: claim idempotency key: %v", ErrUnavailable, err)
	}
	return ok, nil
}

// MarkDone фиксирует успешное завершение операции.
func (s IdempotencyStore) MarkDone(ctx context.Context, key string) error {
	if err := s.client.Set(ctx, key, claimDone, s.ttl).Err(); err != nil {
		return fmt.Errorf("%w: mark idempotency key done: %v", ErrUnavailable, err)
	}
	return nil
}

// Release снимает захват после неуспешной операции, позволяя клиенту повторить
// запрос с тем же ключом.
func (s IdempotencyStore) Release(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("%w: release idempotency key: %v", ErrUnavailable, err)
	}
	return nil
}

const (
	claimInProgress = "in_progress"
	claimDone       = "done"
)

// EventDeduplicator — кеш обработанных событий. Служит быстрым фильтром перед
// обращением к processed_events и не является источником истины.
type EventDeduplicator struct {
	client *redis.Client
	ttl    time.Duration
}

func NewEventDeduplicator(client *redis.Client, ttl time.Duration) EventDeduplicator {
	return EventDeduplicator{client: client, ttl: ttl}
}

func (d EventDeduplicator) IsProcessed(ctx context.Context, consumerName, eventID string) (bool, error) {
	exists, err := d.client.Exists(ctx, eventKey(consumerName, eventID)).Result()
	if err != nil {
		return false, fmt.Errorf("%w: check processed event: %v", ErrUnavailable, err)
	}
	return exists > 0, nil
}

func (d EventDeduplicator) MarkProcessed(ctx context.Context, consumerName, eventID string) error {
	if err := d.client.Set(ctx, eventKey(consumerName, eventID), "1", d.ttl).Err(); err != nil {
		return fmt.Errorf("%w: mark processed event: %v", ErrUnavailable, err)
	}
	return nil
}

func eventKey(consumerName, eventID string) string {
	return "event:" + consumerName + ":" + eventID
}
