package http

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/appctx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/middleware"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/usecase"
)

type Handler struct {
	payments  *usecase.PaymentUsecase
	metrics   *observability.Metrics
	jwtSecret string
}

// payRequest управляет только режимом симуляции платёжного провайдера.
// Сумма приходит из заказа и клиентом не задаётся.
type payRequest struct {
	Simulation string `json:"simulate"`
}

type paymentResponse struct {
	ID        string `json:"id"`
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Amount    string `json:"amount"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func NewHandler(payments *usecase.PaymentUsecase, metrics *observability.Metrics, jwtSecret string) *Handler {
	return &Handler{payments: payments, metrics: metrics, jwtSecret: jwtSecret}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	auth := middleware.Auth(h.jwtSecret)

	mux.Handle("POST /api/orders/{id}/pay", auth(http.HandlerFunc(h.pay)))
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", health)
}

func health(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) pay(w http.ResponseWriter, r *http.Request) {
	user, ok := appctx.UserFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "unauthorized")
		return
	}

	var req payRequest
	if err := httpx.DecodeOptionalJSON(w, r, &req); err != nil {
		httpx.WriteDecodeError(w, err)
		return
	}

	simulation, err := normalizeSimulation(req.Simulation)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, err.Error())
		return
	}

	payment, err := h.payments.Create(r.Context(), usecase.CreatePaymentInput{
		OrderID:        r.PathValue("id"),
		UserID:         user.ID,
		IdempotencyKey: idempotencyKey(r),
		Simulation:     simulation,
	})
	if err != nil {
		h.metrics.PaymentsTotal.WithLabelValues("error").Inc()
		writePaymentError(w, err)
		return
	}

	h.metrics.PaymentsTotal.WithLabelValues(payment.Status).Inc()
	httpx.WriteJSON(w, http.StatusCreated, toPaymentResponse(payment))
}

// normalizeSimulation принимает только известные значения: неизвестный режим
// должен быть явной ошибкой, а не молча трактоваться как успешная оплата.
func normalizeSimulation(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "success", "succeeded", "ok":
		return usecase.SimulationSuccess, nil
	case "failure", "failed", "fail":
		return usecase.SimulationFailure, nil
	default:
		return "", errors.New(`"simulate" must be either "success" or "failure"`)
	}
}

func writePaymentError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecase.ErrOrderNotFound):
		httpx.WriteError(w, http.StatusNotFound, httpx.CodeNotFound, "order not found")

	case errors.Is(err, usecase.ErrPaymentNotFound):
		httpx.WriteError(w, http.StatusNotFound, httpx.CodeNotFound, "payment not found")

	case errors.Is(err, usecase.ErrOrderNotPayable):
		httpx.WriteError(w, http.StatusConflict, httpx.CodeConflict,
			"order is not in a payable state")

	case errors.Is(err, usecase.ErrActivePaymentExists):
		httpx.WriteError(w, http.StatusConflict, httpx.CodeConflict,
			"order already has an active payment")

	case errors.Is(err, usecase.ErrIdempotencyInFlight):
		w.Header().Set("Retry-After", "1")
		httpx.WriteError(w, http.StatusConflict, httpx.CodeConflict,
			"request with the same idempotency key is still in progress, retry shortly")

	default:
		httpx.WriteError(w, http.StatusInternalServerError, httpx.CodeInternal, "internal error")
	}
}

func toPaymentResponse(payment domain.Payment) paymentResponse {
	return paymentResponse{
		ID:        payment.ID,
		OrderID:   payment.OrderID,
		UserID:    payment.UserID,
		Amount:    payment.Amount.String(),
		Status:    payment.Status,
		CreatedAt: payment.CreatedAt.Format(time.RFC3339),
		UpdatedAt: payment.UpdatedAt.Format(time.RFC3339),
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
