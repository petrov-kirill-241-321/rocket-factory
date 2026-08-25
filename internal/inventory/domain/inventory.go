package domain

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
)

// Жизненный цикл резерва.
//
// Раньше существовали только reserved и failed, а quantity_reserved никогда не
// уменьшался: остатки «вытекали» при каждой неуспешной оплате и не списывались
// после производства. Released и committed закрывают обе ветки.
const (
	// ReservationStatusReserved — остатки удержаны под заказ.
	ReservationStatusReserved = "reserved"
	// ReservationStatusFailed — удержать не удалось, остатки не менялись.
	ReservationStatusFailed = "failed"
	// ReservationStatusReleased — удержание снято (оплата не прошла).
	ReservationStatusReleased = "released"
	// ReservationStatusCommitted — удержание превращено в списание (производство завершено).
	ReservationStatusCommitted = "committed"
)

var (
	ErrEmptyReservation = errors.New("reservation must contain at least one item")
	ErrInvalidQuantity  = errors.New("item quantity must be positive")
	ErrInvalidSKU       = errors.New("item sku is required")
)

// MaxItemsPerReservation ограничивает размер одной операции со складом.
const MaxItemsPerReservation = 50

type Item struct {
	ID                string
	SKU               string
	Name              string
	QuantityAvailable int
	QuantityReserved  int
	UnitPrice         money.Amount
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Reservation struct {
	ID        string
	OrderID   string
	UserID    string
	Status    string
	Items     []ReservationItem
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ReservationItem struct {
	SKU      string
	Name     string
	Quantity int
}

type Availability struct {
	SKU       string
	Requested int
	Available int
	Enough    bool
}

// NormalizeItems приводит позиции к каноническому виду и сортирует их по SKU.
//
// Сортировка — не косметика: строки inventory_items блокируются в порядке
// перебора позиций, и без единого порядка два параллельных заказа с
// пересекающимся набором SKU уходят во взаимную блокировку.
func NormalizeItems(items []ReservationItem) ([]ReservationItem, error) {
	if len(items) == 0 {
		return nil, ErrEmptyReservation
	}
	if len(items) > MaxItemsPerReservation {
		return nil, ErrEmptyReservation
	}

	merged := make(map[string]ReservationItem, len(items))
	for _, item := range items {
		sku := strings.ToUpper(strings.TrimSpace(item.SKU))
		if sku == "" {
			return nil, ErrInvalidSKU
		}
		if item.Quantity <= 0 {
			return nil, ErrInvalidQuantity
		}
		existing, ok := merged[sku]
		if !ok {
			merged[sku] = ReservationItem{SKU: sku, Name: strings.TrimSpace(item.Name), Quantity: item.Quantity}
			continue
		}
		existing.Quantity += item.Quantity
		merged[sku] = existing
	}

	out := make([]ReservationItem, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out, nil
}
