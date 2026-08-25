package domain

import (
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
)

const (
	StatusPending   = "pending"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

// PayableOrderStatuses — состояния заказа, в которых оплата допустима.
var PayableOrderStatuses = map[string]bool{
	"inventory_reserved": true,
	"payment_pending":    true,
}

type Payment struct {
	ID             string
	OrderID        string
	UserID         string
	Status         string
	Amount         money.Amount
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// IsActive показывает, занимает ли платёж «слот» заказа.
// На один заказ допускается только один активный платёж — это гарантируется
// частичным уникальным индексом payments_order_id_active_uq.
func (p Payment) IsActive() bool {
	return p.Status == StatusPending || p.Status == StatusSucceeded
}
