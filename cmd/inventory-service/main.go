package main

import (
	"context"
	"net/http"
	"os"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/app"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/config"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/grpcx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
	invrepo "github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/repository"
	invgrpc "github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/transport/grpc"
	invkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/transport/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/usecase"
	sharedkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/middleware"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/postgres"
	sharedredis "github.com/petrov-kirill-241-321/rocket-factory/internal/redis"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
)

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString("inventory-service: " + err.Error() + "\n")
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

	inventory := usecase.NewInventoryUsecase(invrepo.NewPostgresRepository(db), cfg.Kafka.InventoryTopic)

	producer := sharedkafka.NewDynamicProducer(cfg.Kafka.Brokers)
	publisher := outbox.NewPublisher(outbox.NewStore(db), producer, logger, metrics, outbox.Config{})

	consumer := sharedkafka.NewConsumer(
		sharedkafka.ConsumerConfig{
			Brokers: cfg.Kafka.Brokers,
			// order_created удерживает остатки, payment_failed возвращает их
			// в продажу, production_completed списывает. Без двух последних
			// топиков удержания копились и склад исчерпывался.
			Topics: []string{
				cfg.Kafka.OrderTopic,
				cfg.Kafka.PaymentTopic,
				cfg.Kafka.ProductionTopic,
			},
			GroupID:      "inventory-service",
			ConsumerName: "inventory-service",
			DLQTopic:     cfg.Kafka.DLQTopic,
			MaxAttempts:  cfg.Kafka.MaxAttempts,
			RetryBackoff: cfg.Kafka.RetryBackoff,
		},
		invkafka.NewHandler(inventory, logger, metrics),
		sharedredis.NewEventDeduplicator(redisClient, cfg.Redis.TTL),
		producer,
		logger,
		metrics,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", health)

	grpcServer := grpcx.NewServer(logger, metrics)
	rocketpb.RegisterInventoryServiceServer(grpcServer, invgrpc.NewServer(inventory))

	runner := app.NewRunner(logger)
	runner.HTTP(cfg.HTTPAddr, middleware.Chain(mux, logger, cfg.ServiceName, metrics))
	runner.GRPC(cfg.GRPCAddr, grpcServer)
	runner.Background("outbox publisher", func(ctx context.Context) error {
		publisher.Run(ctx)
		return nil
	})
	runner.Background("kafka consumer", consumer.Run)

	runner.OnShutdown("kafka consumer", func(context.Context) error { return consumer.Close() })
	runner.OnShutdown("kafka producer", func(context.Context) error { return producer.Close() })
	runner.OnShutdown("redis", func(context.Context) error { return redisClient.Close() })
	runner.OnShutdown("postgres", func(context.Context) error { db.Close(); return nil })
	runner.OnShutdown("tracing", shutdownTracing)

	return runner.Run(ctx)
}

func health(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
