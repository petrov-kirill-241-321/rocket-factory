package usecase

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/repository"
)

var (
	ErrEmptyOrder      = errors.New("order must contain at least one item")
	ErrTooManyItems    = fmt.Errorf("order must contain at most %d items", domain.MaxItemsPerOrder)
	ErrInvalidItem     = errors.New("item sku is required")
	ErrInvalidQuantity = errors.New("item quantity must be positive")
	ErrDuplicateSKU    = errors.New("order must not contain duplicate skus")
	ErrUnknownSKU      = errors.New("unknown or inactive sku")
	ErrOrderNotFound   = repository.ErrOrderNotFound
	// ErrIdempotencyInFlight — операция с тем же ключом ещё выполняется.
	// Клиенту следует повторить запрос позже, а не считать заказ несуществующим.
	ErrIdempotencyInFlight = errors.New("request with the same idempotency key is in flight")
)

// IdempotencyStore защищает от дублей на уровне запроса. Источником истины
// остаётся уникальный индекс orders(user_id, idempotency_key).
type IdempotencyStore interface {
	Claim(ctx context.Context, key string) (bool, error)
	MarkDone(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
}

type OrderUsecase struct {
	orders      repository.Repository
	catalog     repository.CatalogRepository
	idempotency IdempotencyStore
	orderTopic  string
}

type CreateOrderInput struct {
	UserID         string
	IdempotencyKey string
	Items          []CreateOrderItem
}

// CreateOrderItem не содержит цены: её определяет каталог на стороне сервиса.
type CreateOrderItem struct {
	SKU      string
	Quantity int
}

func NewOrderUsecase(
	orders repository.Repository,
	catalog repository.CatalogRepository,
	idempotency IdempotencyStore,
	orderTopic string,
) *OrderUsecase {
	return &OrderUsecase{orders: orders, catalog: catalog, idempotency: idempotency, orderTopic: orderTopic}
}

func (u *OrderUsecase) Create(ctx context.Context, input CreateOrderInput) (order domain.Order, err error) {
	items, err := normalizeItems(input.Items)
	if err != nil {
		return domain.Order{}, err
	}

	if input.IdempotencyKey != "" {
		key := idempotencyKey(input.UserID, input.IdempotencyKey)

		claimed, claimErr := u.idempotency.Claim(ctx, key)
		if claimErr != nil {
			// Redis недоступен. Продолжаем: уникальный индекс в PostgreSQL
			// обеспечивает ту же гарантию, и заказы не должны переставать
			// создаваться из-за отказа вспомогательного кеша.
			claimed = true
		}
		if !claimed {
			existing, findErr := u.orders.FindByIdempotencyKey(ctx, input.UserID, input.IdempotencyKey)
			if findErr == nil {
				return existing, nil
			}
			if errors.Is(findErr, repository.ErrOrderNotFound) {
				// Ключ захвачен, но заказа ещё нет: параллельный запрос в процессе
				// либо предыдущая попытка упала. Отвечаем «повторите», а не 404.
				return domain.Order{}, ErrIdempotencyInFlight
			}
			return domain.Order{}, findErr
		}

		// Ключ выставлен до записи в БД, поэтому при ошибке его нужно снять,
		// иначе клиент не сможет повторить запрос до истечения TTL.
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()

			if err != nil {
				_ = u.idempotency.Release(releaseCtx, key)
				return
			}
			_ = u.idempotency.MarkDone(releaseCtx, key)
		}()
	}

	catalogItems, err := u.resolveCatalog(ctx, items)
	if err != nil {
		return domain.Order{}, err
	}

	order, err = buildOrder(input, items, catalogItems)
	if err != nil {
		return domain.Order{}, err
	}

	event := kafka.NewEvent(kafka.EventOrderCreated, order.ID, order.UserID, map[string]any{
		"items":        eventItems(order.Items),
		"total_amount": order.TotalAmount.String(),
	})

	if createErr := u.orders.Create(ctx, order, event, u.orderTopic); createErr != nil {
		if errors.Is(createErr, repository.ErrDuplicateIdempotKey) && input.IdempotencyKey != "" {
			// Гонка двух параллельных запросов: победил другой. Возвращаем его результат.
			return u.orders.FindByIdempotencyKey(ctx, input.UserID, input.IdempotencyKey)
		}
		return domain.Order{}, createErr
	}

	return order, nil
}

func (u *OrderUsecase) Get(ctx context.Context, orderID, userID string) (domain.Order, error) {
	return u.orders.GetByID(ctx, orderID, userID)
}

func (u *OrderUsecase) List(ctx context.Context, userID string, limit, offset int) ([]domain.Order, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return u.orders.ListByUser(ctx, userID, limit, offset)
}

// ApplyEventStatus применяет статус, пришедший в доменном событии.
//
// Решение о применимости принимается внутри транзакции репозитория под
// блокировкой строки: проверять статус заранее бессмысленно — между чтением
// и обновлением его может изменить параллельный консьюмер.
func (u *OrderUsecase) ApplyEventStatus(
	ctx context.Context,
	event kafka.Event,
	statusValue string,
	consumerName string,
) (repository.StatusUpdate, error) {
	if _, ok := domain.Rank(statusValue); !ok {
		return repository.StatusUpdate{}, domain.ErrUnknownStatus
	}
	return u.orders.ApplyStatus(ctx, repository.ApplyStatusParams{
		OrderID:      event.OrderID,
		Status:       statusValue,
		EventID:      event.EventID,
		EventType:    event.EventType,
		ConsumerName: consumerName,
	})
}

func (u *OrderUsecase) resolveCatalog(ctx context.Context, items []CreateOrderItem) (map[string]domain.CatalogItem, error) {
	skus := make([]string, 0, len(items))
	for _, item := range items {
		skus = append(skus, item.SKU)
	}

	catalogItems, err := u.catalog.FindActiveBySKUs(ctx, skus)
	if err != nil {
		return nil, err
	}

	missing := make([]string, 0)
	for _, sku := range skus {
		if _, ok := catalogItems[sku]; !ok {
			missing = append(missing, sku)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("%w: %s", ErrUnknownSKU, strings.Join(missing, ", "))
	}
	return catalogItems, nil
}

func buildOrder(
	input CreateOrderInput,
	items []CreateOrderItem,
	catalogItems map[string]domain.CatalogItem,
) (domain.Order, error) {
	now := time.Now().UTC()
	order := domain.Order{
		ID:             uuid.NewString(),
		UserID:         input.UserID,
		Status:         domain.StatusCreated,
		IdempotencyKey: input.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
		Items:          make([]domain.Item, 0, len(items)),
	}

	var total money.Amount
	for _, item := range items {
		catalogItem := catalogItems[item.SKU]

		lineTotal, err := catalogItem.UnitPrice.MulQuantity(item.Quantity)
		if err != nil {
			return domain.Order{}, fmt.Errorf("calculate line total for %s: %w", item.SKU, err)
		}
		total, err = total.Add(lineTotal)
		if err != nil {
			return domain.Order{}, fmt.Errorf("calculate order total: %w", err)
		}

		order.Items = append(order.Items, domain.Item{
			ID:        uuid.NewString(),
			OrderID:   order.ID,
			SKU:       catalogItem.SKU,
			Name:      catalogItem.Name,
			Quantity:  item.Quantity,
			UnitPrice: catalogItem.UnitPrice,
			CreatedAt: now,
		})
	}
	order.TotalAmount = total
	return order, nil
}

func normalizeItems(items []CreateOrderItem) ([]CreateOrderItem, error) {
	if len(items) == 0 {
		return nil, ErrEmptyOrder
	}
	if len(items) > domain.MaxItemsPerOrder {
		return nil, ErrTooManyItems
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]CreateOrderItem, 0, len(items))

	for _, item := range items {
		sku := strings.ToUpper(strings.TrimSpace(item.SKU))
		if sku == "" {
			return nil, ErrInvalidItem
		}
		if item.Quantity <= 0 {
			return nil, ErrInvalidQuantity
		}
		if _, exists := seen[sku]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateSKU, sku)
		}
		seen[sku] = struct{}{}
		out = append(out, CreateOrderItem{SKU: sku, Quantity: item.Quantity})
	}

	// Стабильный порядок позиций делает детерминированным и порядок блокировок
	// на складе, что исключает взаимные блокировки при параллельных заказах.
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out, nil
}

func idempotencyKey(userID, key string) string {
	return "idempotency:orders:" + userID + ":" + key
}

func eventItems(items []domain.Item) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"sku":        item.SKU,
			"name":       item.Name,
			"quantity":   item.Quantity,
			"unit_price": item.UnitPrice.String(),
		})
	}
	return out
}
