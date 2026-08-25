package middleware

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/appctx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/security"
)

const testSecret = "test-secret-value-32-characters"

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Регресс на кардинальность метрик: во внешних middleware должен быть доступен
// шаблон маршрута, а не путь с конкретным идентификатором.
func TestRouteMiddlewareExposesPatternNotRawPath(t *testing.T) {
	mux := http.NewServeMux()

	var captured string
	mux.HandleFunc("GET /api/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := Route(mux)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = appctx.Route(r.Context(), "unmatched")
		mux.ServeHTTP(w, r)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders/9f3c8b1e-0000-0000-0000-000000000001", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if captured != "GET /api/orders/{id}" {
		t.Fatalf("маршрут = %q, ожидался шаблон GET /api/orders/{id}", captured)
	}
}

func TestRouteMiddlewareMarksUnmatchedRoutes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/orders", func(http.ResponseWriter, *http.Request) {})

	var captured string
	handler := Route(mux)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = appctx.Route(r.Context(), "unmatched")
	}))

	req := httptest.NewRequest(http.MethodGet, "/definitely/not/a/route", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if captured != "unmatched" {
		t.Fatalf("маршрут = %q, ожидалось unmatched", captured)
	}
}

// Регресс на формат ошибок: 401 раньше отдавался как text/plain,
// хотя всё остальное API отвечает JSON.
func TestAuthRejectionUsesJSONErrorFormat(t *testing.T) {
	handler := Auth(testSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	cases := map[string]string{
		"без заголовка":     "",
		"без схемы Bearer":  "some-token",
		"пустой токен":      "Bearer    ",
		"неверная подпись":  "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.invalid",
	}

	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, req)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("код = %d, ожидался 401", recorder.Code)
			}
			if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json; charset=utf-8" {
				t.Fatalf("Content-Type = %q, ожидался JSON", contentType)
			}

			var body httpx.ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("тело не является JSON: %v", err)
			}
			if body.Error.Code != httpx.CodeUnauthorized {
				t.Fatalf("код ошибки = %q, ожидался %q", body.Error.Code, httpx.CodeUnauthorized)
			}
		})
	}
}

func TestAuthAcceptsValidTokenAndPopulatesContext(t *testing.T) {
	token, err := security.IssueJWT("user-1", "pilot@example.com", testSecret, time.Hour)
	if err != nil {
		t.Fatalf("issue jwt: %v", err)
	}

	var user appctx.User
	handler := Auth(testSecret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _ = appctx.UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("код = %d, ожидался 200", recorder.Code)
	}
	if user.ID != "user-1" || user.Email != "pilot@example.com" {
		t.Fatalf("пользователь в контексте = %+v", user)
	}
}

func TestAuthRejectsExpiredToken(t *testing.T) {
	token, err := security.IssueJWT("user-1", "pilot@example.com", testSecret, -time.Hour)
	if err != nil {
		t.Fatalf("issue jwt: %v", err)
	}

	handler := Auth(testSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("код = %d, ожидался 401 для истёкшего токена", recorder.Code)
	}
}

func TestAuthRejectsTokenSignedWithAnotherSecret(t *testing.T) {
	token, err := security.IssueJWT("user-1", "pilot@example.com", "another-secret-value-32-chars", time.Hour)
	if err != nil {
		t.Fatalf("issue jwt: %v", err)
	}

	handler := Auth(testSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("код = %d, ожидался 401", recorder.Code)
	}
}

// Паника не должна ронять процесс и обязана отдавать тот же формат ошибки.
func TestRecoverReturnsJSONAndKeepsServing(t *testing.T) {
	handler := Recover(discardLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("что-то пошло не так")
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/orders", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("код = %d, ожидался 500", recorder.Code)
	}

	var body httpx.ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("тело не является JSON: %v", err)
	}
	if body.Error.Code != httpx.CodeInternal {
		t.Fatalf("код ошибки = %q, ожидался %q", body.Error.Code, httpx.CodeInternal)
	}
}

func TestRequestIDIsGeneratedAndPropagated(t *testing.T) {
	var fromContext string
	handler := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		fromContext = appctx.RequestID(r.Context())
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/orders", nil))

	if fromContext == "" {
		t.Fatal("request_id не сгенерирован")
	}
	if recorder.Header().Get("X-Request-ID") != fromContext {
		t.Fatal("request_id не возвращён в заголовке ответа")
	}
}

func TestRequestIDFromHeaderIsPreserved(t *testing.T) {
	var fromContext string
	handler := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		fromContext = appctx.RequestID(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	req.Header.Set("X-Request-ID", "from-nginx")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if fromContext != "from-nginx" {
		t.Fatalf("request_id = %q, ожидался from-nginx", fromContext)
	}
}
