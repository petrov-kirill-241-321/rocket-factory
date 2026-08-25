package main

import (
	"context"
	"net/http"
	"os"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/app"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/repository"
	authgrpc "github.com/petrov-kirill-241-321/rocket-factory/internal/auth/transport/grpc"
	authhttp "github.com/petrov-kirill-241-321/rocket-factory/internal/auth/transport/http"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/usecase"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/config"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/grpcx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/middleware"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/observability"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/postgres"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
)

func main() {
	if err := run(); err != nil {
		// Логгер может быть ещё не создан, поэтому пишем в stderr напрямую.
		_, _ = os.Stderr.WriteString("auth-service: " + err.Error() + "\n")
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

	users := repository.NewPostgresUserRepository(db)
	authUC := usecase.NewAuthUsecase(users, cfg.JWT.Secret, cfg.JWT.TTL)

	mux := http.NewServeMux()
	authhttp.NewHandler(authUC).RegisterRoutes(mux)
	mux.Handle("GET /metrics", metrics.Handler())

	grpcServer := grpcx.NewServer(logger, metrics)
	rocketpb.RegisterAuthServiceServer(grpcServer, authgrpc.NewServer(authUC))

	runner := app.NewRunner(logger)
	runner.HTTP(cfg.HTTPAddr, middleware.Chain(mux, logger, cfg.ServiceName, metrics))
	runner.GRPC(cfg.GRPCAddr, grpcServer)
	runner.OnShutdown("postgres", func(context.Context) error { db.Close(); return nil })
	runner.OnShutdown("tracing", shutdownTracing)

	return runner.Run(ctx)
}
