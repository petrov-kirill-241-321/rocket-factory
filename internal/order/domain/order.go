package domain

import (
	"errors"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
)

const (
	StatusCreated           = "created"
	StatusInventoryReserved = "inventory_reserved"
	StatusInventoryFailed   = "inventory_failed"
	StatusPaymentPending    = "payment_pending"
	StatusPaid              = "paid"
	StatusProductionStarted = "production_started"
	StatusCompleted         = "completed"
	StatusFailed            = "failed"
)

var (
	ErrUnknownStatus = errors.New("unknown order status")
	ErrOrderIsFinal  = errors.New("order is in a final state")
)

// MaxItemsPerOrder ограничивает размер заказа: без лимита один запрос
// порождает неограниченное количество вставок в одной транзакции.
const MaxItemsPerOrder = 50

// statusRank задаёт монотонный порядок жизненного цикла заказа.
//
// Модель намеренно ранговая, а не «переход из A в B». События приходят из разных
// топиков Kafka, и порядок между топиками не гарантирован: после перезапуска
// order-service вполне может прочитать payment_succeeded раньше inventory_reserved.
// Строгий автомат в этом случае отвергал событие, и заказ навсегда застревал
// в промежуточном статусе. Ранговая модель делает применение статуса
// идемпотентным и независимым от порядка доставки: побеждает самое позднее
// состояние, более ранние события отбрасываются как устаревшие.
var statusRank = map[string]int{
	StatusCreated:           10,
	StatusInventoryReserved: 20,
	StatusPaymentPending:    30,
	StatusPaid:              40,
	StatusProductionStarted: 50,
	StatusCompleted:         60,
	// Отказные состояния терминальны и перекрывают любой успешный прогресс.
	StatusInventoryFailed: 100,
	StatusFailed:          100,
}

var finalStatuses = map[string]bool{
	StatusCompleted:       true,
	StatusFailed:          true,
	StatusInventoryFailed: true,
}

// Rank возвращает позицию статуса в жизненном цикле.
func Rank(status string) (int, bool) {
	rank, ok := statusRank[status]
	return rank, ok
}

// IsFinal сообщает, что заказ достиг терминального состояния.
func IsFinal(status string) bool { return finalStatuses[status] }

// ShouldApply решает, нужно ли применять входящий статус к текущему.
// false без ошибки означает устаревшее или повторное событие — это штатная
// ситуация для доставки at-least-once, а не сбой.
func ShouldApply(current, incoming string) (bool, error) {
	currentRank, ok := Rank(current)
	if !ok {
		return false, ErrUnknownStatus
	}
	incomingRank, ok := Rank(incoming)
	if !ok {
		return false, ErrUnknownStatus
	}
	if IsFinal(current) {
		return false, nil
	}
	return incomingRank > currentRank, nil
}

type Order struct {
	ID             string
	UserID         string
	Status         string
	TotalAmount    money.Amount
	IdempotencyKey string
	Items          []Item
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Item struct {
	ID        string
	OrderID   string
	SKU       string
	Name      string
	Quantity  int
	UnitPrice money.Amount
	CreatedAt time.Time
}

// CatalogItem — позиция каталога. Каталог принадлежит order-service и является
// источником истины по цене: клиент цену не передаёт.
type CatalogItem struct {
	SKU       string
	Name      string
	UnitPrice money.Amount
}
