package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/appctx"
	"go.opentelemetry.io/otel/trace"
)

// NewLogger создаёт структурированный JSON-логгер, который автоматически
// подмешивает в каждую запись request_id, trace_id, span_id и user_id из контекста.
//
// Без этого корреляционные поля попадают только в логи HTTP-middleware,
// а логи консьюмеров и репозиториев остаются без привязки к запросу.
func NewLogger(serviceName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	})
	return slog.New(&contextHandler{Handler: handler}).With("service", serviceName)
}

func logLevel() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type contextHandler struct {
	slog.Handler
}

func (h *contextHandler) Handle(ctx context.Context, record slog.Record) error {
	if requestID := appctx.RequestID(ctx); requestID != "" {
		record.AddAttrs(slog.String("request_id", requestID))
	}
	if user, ok := appctx.UserFromContext(ctx); ok && user.ID != "" {
		record.AddAttrs(slog.String("user_id", user.ID))
	}
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}
