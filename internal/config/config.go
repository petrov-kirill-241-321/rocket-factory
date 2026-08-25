package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// insecureJWTSecret — значение из .env.example. Сервис не должен стартовать с ним
// нигде, кроме локальной разработки.
const insecureJWTSecret = "change-me-local-secret"

type Config struct {
	ServiceName string
	Environment string
	HTTPAddr    string
	GRPCAddr    string

	Postgres   PostgresConfig
	Redis      RedisConfig
	Kafka      KafkaConfig
	JWT        JWTConfig
	OTEL       OTELConfig
	Services   ServicesConfig
	Production ProductionConfig
}

type PostgresConfig struct {
	User            string
	Password        string
	Database        string
	Host            string
	Port            string
	SSLMode         string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	TTL      time.Duration
}

type KafkaConfig struct {
	Brokers         []string
	OrderTopic      string
	InventoryTopic  string
	PaymentTopic    string
	ProductionTopic string
	DLQTopic        string
	MaxAttempts     int
	RetryBackoff    time.Duration
}

type JWTConfig struct {
	Secret string
	TTL    time.Duration
}

type OTELConfig struct {
	Enabled  bool
	Endpoint string
}

type ServicesConfig struct {
	AuthGRPCAddr       string
	OrderGRPCAddr      string
	InventoryGRPCAddr  string
	PaymentGRPCAddr    string
	ProductionGRPCAddr string
}

type ProductionConfig struct {
	Duration          time.Duration
	ReconcileInterval time.Duration
}

func Load() (Config, error) {
	redisDB, err := envInt("REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}
	jwtTTL, err := envDuration("JWT_TTL", 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	redisTTL, err := envDuration("REDIS_KEY_TTL", 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	pgMaxConns, err := envInt("POSTGRES_MAX_CONNS", 10)
	if err != nil {
		return Config{}, err
	}
	pgMinConns, err := envInt("POSTGRES_MIN_CONNS", 2)
	if err != nil {
		return Config{}, err
	}
	pgMaxLifetime, err := envDuration("POSTGRES_MAX_CONN_LIFETIME", time.Hour)
	if err != nil {
		return Config{}, err
	}
	pgMaxIdle, err := envDuration("POSTGRES_MAX_CONN_IDLE_TIME", 30*time.Minute)
	if err != nil {
		return Config{}, err
	}
	kafkaMaxAttempts, err := envInt("KAFKA_CONSUMER_MAX_ATTEMPTS", 5)
	if err != nil {
		return Config{}, err
	}
	kafkaBackoff, err := envDuration("KAFKA_CONSUMER_RETRY_BACKOFF", 200*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	productionDuration, err := envDuration("PRODUCTION_DURATION", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	reconcileInterval, err := envDuration("PRODUCTION_RECONCILE_INTERVAL", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		ServiceName: env("SERVICE_NAME", "rocket-service"),
		Environment: env("APP_ENV", "local"),
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		GRPCAddr:    env("GRPC_ADDR", ":9090"),
		Postgres: PostgresConfig{
			User:     env("POSTGRES_USER", "rocket"),
			Password: env("POSTGRES_PASSWORD", "rocket"),
			Database: env("POSTGRES_DB", "rocket_factory"),
			Host:     env("POSTGRES_HOST", "localhost"),
			// Внутренний порт СУБД, а не порт, проброшенный на хост.
			// Раньше это была одна переменная, и смена порта на хосте ломала подключение.
			Port:            env("POSTGRES_INTERNAL_PORT", "5432"),
			SSLMode:         env("POSTGRES_SSLMODE", "disable"),
			MaxConns:        int32(pgMaxConns),
			MinConns:        int32(pgMinConns),
			MaxConnLifetime: pgMaxLifetime,
			MaxConnIdleTime: pgMaxIdle,
		},
		Redis: RedisConfig{
			Addr:     env("REDIS_ADDR", "localhost:6379"),
			Password: env("REDIS_PASSWORD", ""),
			DB:       redisDB,
			TTL:      redisTTL,
		},
		Kafka: KafkaConfig{
			Brokers:         splitCSV(env("KAFKA_BROKERS", "localhost:9094")),
			OrderTopic:      env("KAFKA_ORDER_EVENTS_TOPIC", "order.events"),
			InventoryTopic:  env("KAFKA_INVENTORY_EVENTS_TOPIC", "inventory.events"),
			PaymentTopic:    env("KAFKA_PAYMENT_EVENTS_TOPIC", "payment.events"),
			ProductionTopic: env("KAFKA_PRODUCTION_EVENTS_TOPIC", "production.events"),
			DLQTopic:        env("KAFKA_DLQ_TOPIC", "rocket.dlq"),
			MaxAttempts:     kafkaMaxAttempts,
			RetryBackoff:    kafkaBackoff,
		},
		JWT: JWTConfig{
			Secret: env("JWT_SECRET", ""),
			TTL:    jwtTTL,
		},
		OTEL: OTELConfig{
			Enabled:  envBool("OTEL_ENABLED", false),
			Endpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
		},
		Services: ServicesConfig{
			AuthGRPCAddr:       env("AUTH_SERVICE_GRPC_ADDR", "localhost:9090"),
			OrderGRPCAddr:      env("ORDER_SERVICE_GRPC_ADDR", "localhost:9090"),
			InventoryGRPCAddr:  env("INVENTORY_SERVICE_GRPC_ADDR", "localhost:9090"),
			PaymentGRPCAddr:    env("PAYMENT_SERVICE_GRPC_ADDR", "localhost:9090"),
			ProductionGRPCAddr: env("PRODUCTION_SERVICE_GRPC_ADDR", "localhost:9090"),
		},
		Production: ProductionConfig{
			Duration:          productionDuration,
			ReconcileInterval: reconcileInterval,
		},
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if strings.TrimSpace(c.JWT.Secret) == "" {
		return errors.New("JWT_SECRET is required")
	}
	if len(c.JWT.Secret) < 16 {
		return errors.New("JWT_SECRET must be at least 16 characters long")
	}
	if c.JWT.Secret == insecureJWTSecret && !c.IsLocal() {
		return errors.New("JWT_SECRET must be changed outside of local environment")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required")
	}
	return nil
}

func (c Config) IsLocal() bool {
	environment := strings.ToLower(c.Environment)
	return environment == "local" || environment == "dev" || environment == "development"
}

func (c PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode,
	)
}

func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) (int, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func envBool(key string, fallback bool) bool {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
