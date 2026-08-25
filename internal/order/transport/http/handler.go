package http

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/appctx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/middleware"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/usecase"
)

type Handler struct {
	orders    *usecase.OrderUsecase
	metrics   *observability.Metrics
	jwtSecret string
}

// createOrderRequest не содержит цену: её определяет каталог на сервере.
// Раньше unit_price приходил от клиента, что позволяло оформить заказ на любую сумму.
type createOrderRequest struct {
	Items []createOrderItemRequest `json:"items"`
}

type createOrderItemRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type orderResponse struct {
	ID          string              `json:"id"`
	UserID      string              `json:"user_id"`
	Status      string              `json:"status"`
	TotalAmount string              `json:"total_amount"`
	Items       []orderItemResponse `json:"items"`
	CreatedAt   string              `json:"created_at"`
	UpdatedAt   string              `json:"updated_at"`
}

type orderItemResponse struct {
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Quantity  int    `json:"quantity"`
	UnitPrice string `json:"unit_price"`
}

type listOrdersResponse struct {
	Orders []orderResponse `json:"orders"`
}

func NewHandler(orders *usecase.OrderUsecase, metrics *observability.Metrics, jwtSecret string) *Handler {
	return &Handler{orders: orders, metrics: metrics, jwtSecret: jwtSecret}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	auth := middleware.Auth(h.jwtSecret)

	mux.Handle("POST /api/orders", auth(http.HandlerFunc(h.create)))
	mux.Handle("GET /api/orders/{id}", auth(http.HandlerFunc(h.get)))
	mux.Handle("GET /api/orders", auth(http.HandlerFunc(h.list)))
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", health)
}

func health(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	user, ok := appctx.UserFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "unauthorized")
		return
	}

	var req createOrderRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteDecodeError(w, err)
		return
	}

	items := make([]usecase.CreateOrderItem, 0, len(req.Items))
	for _, item := range req.Items {
		items = append(items, usecase.CreateOrderItem{SKU: item.SKU, Quantity: item.Quantity})
	}

	order, err := h.orders.Create(r.Context(), usecase.CreateOrderInput{
		UserID:         user.ID,
		IdempotencyKey: idempotencyKey(r),
		Items:          items,
	})
	if err != nil {
		h.metrics.OrdersCreated.WithLabelValues("error").Inc()
		writeOrderError(w, err)
		return
	}

	h.metrics.OrdersCreated.WithLabelValues("success").Inc()
	httpx.WriteJSON(w, http.StatusCreated, toOrderResponse(order))
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	user, ok := appctx.UserFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "unauthorized")
		return
	}

	// Фильтр по user_id в запросе: чужой заказ должен выглядеть как несуществующий,
	// иначе по коду ответа можно перебором узнать чужие идентификаторы.
	order, err := h.orders.Get(r.Context(), r.PathValue("id"), user.ID)
	if err != nil {
		writeOrderError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, toOrderResponse(order))
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	user, ok := appctx.UserFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "unauthorized")
		return
	}

	limit, err := queryInt(r, "limit")
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, "limit must be an integer")
		return
	}
	offset, err := queryInt(r, "offset")
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, "offset must be an integer")
		return
	}

	orders, err := h.orders.List(r.Context(), user.ID, limit, offset)
	if err != nil {
		writeOrderError(w, err)
		return
	}

	response := listOrdersResponse{Orders: make([]orderResponse, 0, len(orders))}
	for _, order := range orders {
		response.Orders = append(response.Orders, toOrderResponse(order))
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func writeOrderError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecase.ErrEmptyOrder),
		errors.Is(err, usecase.ErrTooManyItems),
		errors.Is(err, usecase.ErrInvalidItem),
		errors.Is(err, usecase.ErrInvalidQuantity),
		errors.Is(err, usecase.ErrDuplicateSKU),
		errors.Is(err, usecase.ErrUnknownSKU):
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, err.Error())

	case errors.Is(err, usecase.ErrOrderNotFound), errors.Is(err, repository.ErrOrderNotFound):
		httpx.WriteError(w, http.StatusNotFound, httpx.CodeNotFound, "order not found")

	case errors.Is(err, usecase.ErrIdempotencyInFlight):
		// 409 вместо 404: запрос с тем же ключом ещё выполняется, повтор осмыслен.
		w.Header().Set("Retry-After", "1")
		httpx.WriteError(w, http.StatusConflict, httpx.CodeConflict,
			"request with the same idempotency key is still in progress, retry shortly")

	case errors.Is(err, domain.ErrUnknownStatus):
		httpx.WriteError(w, http.StatusConflict, httpx.CodeConflict, "order is in an unexpected state")

	default:
		// Наружу не отдаём подробности внутренней ошибки.
		httpx.WriteError(w, http.StatusInternalServerError, httpx.CodeInternal, "internal error")
	}
}

func toOrderResponse(order domain.Order) orderResponse {
	items := make([]orderItemResponse, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, orderItemResponse{
			SKU:       item.SKU,
			Name:      item.Name,
			Quantity:  item.Quantity,
			UnitPrice: item.UnitPrice.String(),
		})
	}
	return orderResponse{
		ID:          order.ID,
		UserID:      order.UserID,
		Status:      order.Status,
		TotalAmount: order.TotalAmount.String(),
		Items:       items,
		CreatedAt:   order.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   order.UpdatedAt.Format(time.RFC3339),
	}
}

func idempotencyKey(r *http.Request) string {
	const maxKeyLength = 128
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > maxKeyLength {
		return key[:maxKeyLength]
	}
	return key
}

func queryInt(r *http.Request, name string) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, nil
	}
	return strconv.Atoi(raw)
}
