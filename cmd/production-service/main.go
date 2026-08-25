package main

import (
	"context"
	"net/http"
	"os"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/app"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/config"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/grpcx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/httpx"
	sharedkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/middleware"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/outbox"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/postgres"
	prodrepo "github.com/petrov-kirill-241-321/rocket-factory/internal/production/repository"
	prodgrpc "github.com/petrov-kirill-241-321/rocket-factory/internal/production/transport/grpc"
	prodkafka "github.com/petrov-kirill-241-321/rocket-factory/internal/production/transport/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/usecase"
	sharedredis "github.com/petrov-kirill-241-321/rocket-factory/internal/redis"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
)

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString("production-service: " + err.Error() + "\n")
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

	production := usecase.NewProductionUsecase(
		prodrepo.NewPostgresRepository(db),
		logger, metrics,
		cfg.Kafka.ProductionTopic,
		cfg.Production.Duration,
	)
	// Завершение задач вынесено в фоновый цикл, а не в горутину со Sleep:
	// после перезапуска сервиса «зависшие» задачи будут завершены сами.
	reconciler := usecase.NewReconciler(production, logger, cfg.Production.ReconcileInterval)

	producer := sharedkafka.NewDynamicProducer(cfg.Kafka.Brokers)
	publisher := outbox.NewPublisher(outbox.NewStore(db), producer, logger, metrics, outbox.Config{})

	consumer := sharedkafka.NewConsumer(
		sharedkafka.ConsumerConfig{
			Brokers:      cfg.Kafka.Brokers,
			Topics:       []string{cfg.Kafka.PaymentTopic},
			GroupID:      "production-service",
			ConsumerName: "production-service",
			DLQTopic:     cfg.Kafka.DLQTopic,
			MaxAttempts:  cfg.Kafka.MaxAttempts,
			RetryBackoff: cfg.Kafka.RetryBackoff,
		},
		prodkafka.NewHandler(production, logger, metrics),
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
	rocketpb.RegisterProductionServiceServer(grpcServer, prodgrpc.NewServer(production))

	runner := app.NewRunner(logger)
	runner.HTTP(cfg.HTTPAddr, middleware.Chain(mux, logger, cfg.ServiceName, metrics))
	runner.GRPC(cfg.GRPCAddr, grpcServer)
	runner.Background("outbox publisher", func(ctx context.Context) error {
		publisher.Run(ctx)
		return nil
	})
	runner.Background("kafka consumer", consumer.Run)
	runner.Background("production reconciler", reconciler.Run)

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
