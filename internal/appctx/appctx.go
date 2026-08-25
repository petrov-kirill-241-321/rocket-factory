// Package appctx хранит значения, которые переносятся через context.Context
// между транспортом, наблюдаемостью и бизнес-логикой.
//
// Вынесен в отдельный пакет, чтобы middleware и observability могли
// пользоваться одними ключами без циклического импорта.
package appctx

import "context"

type (
	requestIDKey struct{}
	routeKey     struct{}
	userKey      struct{}
)

// User — аутентифицированный пользователь запроса.
type User struct {
	ID    string
	Email string
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

// WithRoute сохраняет шаблон маршрута ("/api/orders/{id}"), а не конкретный путь.
func WithRoute(ctx context.Context, route string) context.Context {
	return context.WithValue(ctx, routeKey{}, route)
}

func Route(ctx context.Context, fallback string) string {
	value, _ := ctx.Value(routeKey{}).(string)
	if value == "" {
		return fallback
	}
	return value
}

func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userKey{}, user)
}

func UserFromContext(ctx context.Context) (User, bool) {
	user, ok := ctx.Value(userKey{}).(User)
	return user, ok
}
