package main

import (
	"context"
	"net/http"
	"os"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/app"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/config"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/grpcx"
	sharedkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/middleware"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
	paygateway "github.com/petrov-kirill-241-321/rocket-factory/internal/payment/gateway"
	payrepo "github.com/petrov-kirill-241-321/rocket-factory/internal/payment/repository"
	paygrpc "github.com/petrov-kirill-241-321/rocket-factory/internal/payment/transport/grpc"
	payhttp "github.com/petrov-kirill-241-321/rocket-factory/internal/payment/transport/http"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/usecase"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/postgres"
	sharedredis "github.com/petrov-kirill-241-321/rocket-factory/internal/redis"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
)

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString("payment-service: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.ServiceName)
	metrics := observability.NewMetrics(cfg.ServiceName)

	shutdownTracing, err := observability.InitTracing(ctx, cfg.ServiceName, cfg.Environment, cfg.OTEL.Endpoint, cfg.OTEL.Enabled)
	if err != nil {
		return err
	}

	db, err := postgres.Connect(ctx, cfg.Postgres.DSN(), postgres.Options{
		MaxConns:        cfg.Postgres.MaxConns,
		MinConns:        cfg.Postgres.MinConns,
		MaxConnLifetime: cfg.Postgres.MaxConnLifetime,
		MaxConnIdleTime: cfg.Postgres.MaxConnIdleTime,
	})
	if err != nil {
		return err
	}
	if err := metrics.Register(postgres.NewPoolCollector(db, cfg.ServiceName)); err != nil {
		logger.Warn("register pool metrics", "error", err)
	}

	redisClient, err := sharedredis.Connect(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		return err
	}

	// Соединение устанавливается лениво: payment-service поднимается
	// независимо от готовности order-service.
	orderGateway, err := paygateway.NewOrderGRPCGateway(cfg.Services.OrderGRPCAddr)
	if err != nil {
		return err
	}

	payments := usecase.NewPaymentUsecase(
		payrepo.NewPostgresRepository(db),
		sharedredis.NewIdempotencyStore(redisClient, cfg.Redis.TTL),
		orderGateway,
		cfg.Kafka.PaymentTopic,
	)

	producer := sharedkafka.NewDynamicProducer(cfg.Kafka.Brokers)
	publisher := outbox.NewPublisher(outbox.NewStore(db), producer, logger, metrics, outbox.Config{})

	mux := http.NewServeMux()
	payhttp.NewHandler(payments, metrics, cfg.JWT.Secret).RegisterRoutes(mux)
	mux.Handle("GET /metrics", metrics.Handler())

	grpcServer := grpcx.NewServer(logger, metrics)
	rocketpb.RegisterPaymentServiceServer(grpcServer, paygrpc.NewServer(payments))

	runner := app.NewRunner(logger)
	runner.HTTP(cfg.HTTPAddr, middleware.Chain(mux, logger, cfg.ServiceName, metrics))
	runner.GRPC(cfg.GRPCAddr, grpcServer)
	runner.Background("outbox publisher", func(ctx context.Context) error {
		publisher.Run(ctx)
		return nil
	})

	runner.OnShutdown("kafka producer", func(context.Context) error { return producer.Close() })
	runner.OnShutdown("order gateway", func(context.Context) error { return orderGateway.Close() })
	runner.OnShutdown("redis", func(context.Context) error { return redisClient.Close() })
	runner.OnShutdown("postgres", func(context.Context) error { db.Close(); return nil })
	runner.OnShutdown("tracing", shutdownTracing)

	return runner.Run(ctx)
}
