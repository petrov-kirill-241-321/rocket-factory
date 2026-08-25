// Package grpcx настраивает gRPC-сервер и клиентов: трейсинг, метрики,
// логирование, восстановление после паники и разумные таймауты.
//
// Раньше grpc.NewServer() создавался без единой опции: паника в обработчике
// роняла процесс, вызовы не попадали ни в метрики, ни в трейсы, а trace context
// не переходил границу сервиса — трасса заказа обрывалась на первом же вызове.
package grpcx

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxMessageBytes = 4 << 20 // 4 MiB

// metadataCarrier переносит trace context через метаданные gRPC-вызова.
type metadataCarrier struct {
	md *metadata.MD
}

func (c metadataCarrier) Get(key string) string {
	values := c.md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c metadataCarrier) Set(key, value string) {
	c.md.Set(key, value)
}

func (c metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.md))
	for key := range *c.md {
		keys = append(keys, key)
	}
	return keys
}

// NewServer создаёт gRPC-сервер со стандартной обвязкой и health-сервисом.
func NewServer(logger *slog.Logger, metrics *observability.Metrics) *grpc.Server {
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			recoveryInterceptor(logger),
			tracingInterceptor(),
			observabilityInterceptor(logger, metrics),
		),
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	// Health-сервис нужен как для docker-compose, так и для оркестраторов.
	healthpb.RegisterHealthServer(server, health.NewServer())
	return server
}

// NewClient создаёт клиентское соединение.
//
// grpc.DialContext устарел; grpc.NewClient подключается лениво, поэтому
// сервис-потребитель стартует независимо от готовности сервиса-поставщика.
func NewClient(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(clientTracingInterceptor()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
}

func recoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(ctx, "grpc panic recovered",
					"method", info.FullMethod,
					"panic", recovered,
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// tracingInterceptor продолжает трассу вызывающей стороны, а не начинает новую.
func tracingInterceptor() grpc.UnaryServerInterceptor {
	tracer := otel.Tracer("grpc/server")

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}
		ctx = otel.GetTextMapPropagator().Extract(ctx, metadataCarrier{md: &md})

		ctx, span := tracer.Start(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", info.FullMethod),
			))
		defer span.End()

		resp, err := handler(ctx, req)

		code := status.Code(err)
		span.SetAttributes(attribute.String("rpc.grpc.status_code", code.String()))
		if err != nil {
			span.RecordError(err)
			if code == codes.Internal || code == codes.Unavailable || code == codes.Unknown {
				span.SetStatus(otelcodes.Error, err.Error())
			}
		}
		return resp, err
	}
}

// clientTracingInterceptor передаёт trace context вызываемому сервису.
func clientTracingInterceptor() grpc.UnaryClientInterceptor {
	tracer := otel.Tracer("grpc/client")

	return func(
		ctx context.Context,
		method string,
		req, reply any,
		conn *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx, span := tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", method),
			))
		defer span.End()

		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}
		otel.GetTextMapPropagator().Inject(ctx, metadataCarrier{md: &md})
		ctx = metadata.NewOutgoingContext(ctx, md)

		err := invoker(ctx, method, req, reply, conn, opts...)

		code := status.Code(err)
		span.SetAttributes(attribute.String("rpc.grpc.status_code", code.String()))
		if err != nil {
			span.RecordError(err)
		}
		return err
	}
}

func observabilityInterceptor(logger *slog.Logger, metrics *observability.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()

		resp, err := handler(ctx, req)

		code := status.Code(err)
		elapsed := time.Since(started)

		metrics.GRPCRequestCount.WithLabelValues(info.FullMethod, code.String()).Inc()
		metrics.GRPCRequestLatency.WithLabelValues(info.FullMethod).Observe(elapsed.Seconds())

		attrs := []any{
			"method", info.FullMethod,
			"code", code.String(),
			"latency_ms", elapsed.Milliseconds(),
		}
		if err != nil {
			logger.ErrorContext(ctx, "grpc request", append(attrs, "error", err)...)
		} else {
			logger.InfoContext(ctx, "grpc request", attrs...)
		}
		return resp, err
	}
}
