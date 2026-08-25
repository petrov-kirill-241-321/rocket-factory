package http

import (
	"errors"
	"net/http"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/usecase"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
)

type Handler struct {
	auth *usecase.AuthUsecase
}

type authRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type authResponse struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Token  string `json:"token"`
}

func NewHandler(auth *usecase.AuthUsecase) *Handler {
	return &Handler{auth: auth}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/auth/register", h.register)
	mux.HandleFunc("POST /api/auth/login", h.login)
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", health)
}

func health(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var req authRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteDecodeError(w, err)
		return
	}

	result, err := h.auth.Register(r.Context(), req.Email, req.Password)
	if err != nil {
		writeAuthError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, toAuthResponse(result))
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	var req authRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteDecodeError(w, err)
		return
	}

	result, err := h.auth.Login(r.Context(), req.Email, req.Password)
	if err != nil {
		writeAuthError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, toAuthResponse(result))
}

func writeAuthError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecase.ErrInvalidEmail):
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, "invalid email")
	case errors.Is(err, usecase.ErrWeakPassword):
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, usecase.ErrWeakPassword.Error())
	case errors.Is(err, usecase.ErrPasswordTooLong):
		httpx.WriteError(w, http.StatusBadRequest, httpx.CodeValidation, usecase.ErrPasswordTooLong.Error())
	case errors.Is(err, repository.ErrEmailAlreadyTaken):
		httpx.WriteError(w, http.StatusConflict, httpx.CodeConflict, "email already taken")
	case errors.Is(err, usecase.ErrInvalidCredentials):
		// Единая формулировка для несуществующего пользователя и неверного пароля:
		// иначе по ответу можно перебором выяснить, какие адреса зарегистрированы.
		httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "invalid credentials")
	default:
		httpx.WriteError(w, http.StatusInternalServerError, httpx.CodeInternal, "internal error")
	}
}

// toAuthResponse собирается явно: конверсия структур молча ломается,
// если в одной из них изменится порядок или набор полей.
func toAuthResponse(result usecase.AuthResult) authResponse {
	return authResponse{
		UserID: result.UserID,
		Email:  result.Email,
		Token:  result.Token,
	}
}
