package middleware

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/appctx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/security"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Chain собирает цепочку middleware в правильном порядке.
//
// Порядок важен: Route должен идти до Tracing и Metrics, иначе они не увидят
// шаблон маршрута; Recover ставится ближе всех к обработчику, чтобы внешние
// слои успели зафиксировать статус 500.
func Chain(mux *http.ServeMux, logger *slog.Logger, serviceName string, metrics httpMetrics) http.Handler {
	var handler http.Handler = mux
	handler = Recover(logger)(handler)
	handler = metrics.HTTPMiddleware(handler)
	handler = Logging(logger)(handler)
	handler = Tracing(serviceName)(handler)
	handler = Route(mux)(handler)
	handler = RequestID(handler)
	return handler
}

// httpMetrics описывает минимальный контракт метрик, нужный цепочке.
type httpMetrics interface {
	HTTPMiddleware(next http.Handler) http.Handler
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// RequestID берёт идентификатор из заголовка (его проставляет Nginx) либо генерирует.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(appctx.WithRequestID(r.Context(), requestID)))
	})
}

// Route кладёт в контекст шаблон маршрута ("POST /api/orders/{id}/pay").
//
// http.ServeMux заполняет Request.Pattern на клоне запроса, который получает
// только конечный обработчик, поэтому внешние middleware его не видят и
// вынуждены были бы использовать сырой путь — это взрывает кардинальность
// метрик и имён спанов. Публичный ServeMux.Handler даёт шаблон заранее.
func Route(mux *http.ServeMux) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, pattern := mux.Handler(r)
			if pattern == "" {
				pattern = "unmatched"
			}
			next.ServeHTTP(w, r.WithContext(appctx.WithRoute(r.Context(), pattern)))
		})
	}
}

func Logging(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(recorder, r)

			// request_id, trace_id и user_id добавляются обработчиком логов из контекста.
			logger.InfoContext(r.Context(), "http request",
				"method", r.Method,
				"path", r.URL.Path,
				"route", appctx.Route(r.Context(), "unmatched"),
				"status", recorder.status,
				"latency_ms", time.Since(started).Milliseconds(),
			)
		})
	}
}

func Tracing(serviceName string) func(http.Handler) http.Handler {
	tracer := otel.Tracer(serviceName + "/http")

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			route := appctx.Route(ctx, "unmatched")

			ctx, span := tracer.Start(ctx, r.Method+" "+route,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("http.request.method", r.Method),
					attribute.String("http.route", route),
					attribute.String("url.path", r.URL.Path),
					attribute.String("request.id", appctx.RequestID(ctx)),
				))
			defer span.End()

			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(recorder, r.WithContext(ctx))

			span.SetAttributes(attribute.Int("http.response.status_code", recorder.status))
			if recorder.status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(recorder.status))
			}
		})
	}
}

func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.ErrorContext(r.Context(), "panic recovered", "panic", recovered)
					httpx.WriteError(w, http.StatusInternalServerError, httpx.CodeInternal, "internal error")
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// Auth проверяет Bearer-токен и кладёт пользователя в контекст.
// Ошибки отдаются в том же JSON-формате, что и все остальные ответы API.
func Auth(jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if header == "" {
				httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "authorization header is required")
				return
			}

			tokenValue, ok := strings.CutPrefix(header, "Bearer ")
			if !ok || strings.TrimSpace(tokenValue) == "" {
				httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "authorization header must use the Bearer scheme")
				return
			}

			claims, err := security.ParseJWT(strings.TrimSpace(tokenValue), jwtSecret)
			if err != nil {
				httpx.WriteError(w, http.StatusUnauthorized, httpx.CodeUnauthorized, "invalid or expired token")
				return
			}

			ctx := appctx.WithUser(r.Context(), appctx.User{ID: claims.UserID, Email: claims.Email})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
