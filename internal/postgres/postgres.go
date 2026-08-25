package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Options задаёт параметры пула соединений. Дефолты pgxpool (MaxConns = NumCPU,
// без ограничения времени жизни соединения) не подходят для сервиса,
// работающего с пулером и перезапускаемой БД.
type Options struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

func Connect(ctx context.Context, dsn string, opts Options) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	if opts.MinConns > 0 {
		cfg.MinConns = opts.MinConns
	}
	if opts.MaxConnLifetime > 0 {
		cfg.MaxConnLifetime = opts.MaxConnLifetime
	}
	if opts.MaxConnIdleTime > 0 {
		cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	}
	cfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}

// NewPoolCollector экспортирует состояние пула в Prometheus: насыщение пула —
// одна из первых причин роста латентности, и её нужно видеть.
func NewPoolCollector(pool *pgxpool.Pool, serviceName string) prometheus.Collector {
	labels := prometheus.Labels{"service": serviceName}

	return &poolCollector{
		pool: pool,
		acquired: prometheus.NewDesc(
			"rocket_factory_pgx_acquired_conns",
			"Currently acquired connections.", nil, labels),
		idle: prometheus.NewDesc(
			"rocket_factory_pgx_idle_conns",
			"Currently idle connections.", nil, labels),
		total: prometheus.NewDesc(
			"rocket_factory_pgx_total_conns",
			"Total connections in the pool.", nil, labels),
		max: prometheus.NewDesc(
			"rocket_factory_pgx_max_conns",
			"Maximum pool size.", nil, labels),
		emptyAcquire: prometheus.NewDesc(
			"rocket_factory_pgx_empty_acquire_total",
			"Acquires that had to wait for a free connection.", nil, labels),
	}
}

type poolCollector struct {
	pool         *pgxpool.Pool
	acquired     *prometheus.Desc
	idle         *prometheus.Desc
	total        *prometheus.Desc
	max          *prometheus.Desc
	emptyAcquire *prometheus.Desc
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquired
	ch <- c.idle
	ch <- c.total
	ch <- c.max
	ch <- c.emptyAcquire
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	stat := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(stat.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(stat.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(stat.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(stat.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquire, prometheus.CounterValue, float64(stat.EmptyAcquireCount()))
}
