package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
)

// Ошибки валидации живут в domain: это правила предметной области,
// а не особенности конкретного транспорта.
var (
	ErrEmptyReservation = domain.ErrEmptyReservation
	ErrInvalidQuantity  = domain.ErrInvalidQuantity
)

type InventoryUsecase struct {
	repo           repository.Repository
	inventoryTopic string
}

func NewInventoryUsecase(repo repository.Repository, inventoryTopic string) *InventoryUsecase {
	return &InventoryUsecase{repo: repo, inventoryTopic: inventoryTopic}
}

func (u *InventoryUsecase) CheckAvailability(ctx context.Context, items []domain.ReservationItem) ([]domain.Availability, error) {
	normalized, err := domain.NormalizeItems(items)
	if err != nil {
		return nil, err
	}
	return u.repo.CheckAvailability(ctx, normalized)
}

// ReserveForOrder удерживает остатки под заказ.
func (u *InventoryUsecase) ReserveForOrder(
	ctx context.Context,
	orderID, userID string,
	items []domain.ReservationItem,
	event repository.EventContext,
) (domain.Reservation, error) {
	normalized, err := domain.NormalizeItems(items)
	if err != nil {
		return domain.Reservation{}, err
	}

	now := time.Now().UTC()
	return u.repo.Reserve(ctx, repository.ReserveParams{
		Reservation: domain.Reservation{
			ID:        uuid.NewString(),
			OrderID:   orderID,
			UserID:    userID,
			Items:     normalized,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Event:       event,
		OutboxEvent: kafka.NewEvent(kafka.EventInventoryReserved, orderID, userID, map[string]any{}),
		OutboxTopic: u.inventoryTopic,
	})
}

// ReleaseForOrder возвращает удержанные остатки в продажу: оплата не прошла.
// Без этого шага склад безвозвратно «вытекал» при каждой неуспешной оплате.
func (u *InventoryUsecase) ReleaseForOrder(
	ctx context.Context,
	orderID, userID, reason string,
	event repository.EventContext,
) (domain.Reservation, error) {
	return u.repo.Release(ctx, repository.SettleParams{
		OrderID:     orderID,
		Reason:      reason,
		Event:       event,
		OutboxEvent: kafka.NewEvent(kafka.EventInventoryReleased, orderID, userID, map[string]any{}),
		OutboxTopic: u.inventoryTopic,
	})
}

// CommitForOrder списывает удержанные остатки: производство завершено.
func (u *InventoryUsecase) CommitForOrder(
	ctx context.Context,
	orderID, userID string,
	event repository.EventContext,
) (domain.Reservation, error) {
	return u.repo.Commit(ctx, repository.SettleParams{
		OrderID:     orderID,
		Event:       event,
		OutboxEvent: kafka.NewEvent(kafka.EventInventoryCommitted, orderID, userID, map[string]any{}),
		OutboxTopic: u.inventoryTopic,
	})
}
