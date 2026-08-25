package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/appctx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics — реестр метрик сервиса.
//
// Имя сервиса вынесено в константный лейбл, а не в Subsystem: иначе у каждого
// сервиса получается собственное имя метрики и агрегировать их в Grafana нельзя.
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequestCount   *prometheus.CounterVec
	HTTPRequestLatency *prometheus.HistogramVec
	HTTPErrorCount     *prometheus.CounterVec

	GRPCRequestCount   *prometheus.CounterVec
	GRPCRequestLatency *prometheus.HistogramVec

	KafkaProcessedCount     *prometheus.CounterVec
	KafkaSkippedCount       *prometheus.CounterVec
	KafkaErrorCount         *prometheus.CounterVec
	KafkaDLQCount           *prometheus.CounterVec
	KafkaProcessingDuration *prometheus.HistogramVec

	OrdersCreated       *prometheus.CounterVec
	OrderStatusApplied  *prometheus.CounterVec
	PaymentsTotal       *prometheus.CounterVec
	ReservationsTotal   *prometheus.CounterVec
	ProductionTasks     *prometheus.CounterVec
	OutboxPublished     *prometheus.CounterVec
	OutboxPendingTotal  prometheus.Gauge
	OutboxOldestSeconds prometheus.Gauge
}

func NewMetrics(serviceName string) *Metrics {
	registry := prometheus.NewRegistry()
	labels := prometheus.Labels{"service": serviceName}

	counter := func(name, help string, dims ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "rocket_factory",
			Name:        name,
			Help:        help,
			ConstLabels: labels,
		}, dims)
	}
	histogram := func(name, help string, buckets []float64, dims ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "rocket_factory",
			Name:        name,
			Help:        help,
			Buckets:     buckets,
			ConstLabels: labels,
		}, dims)
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "rocket_factory",
			Name:        name,
			Help:        help,
			ConstLabels: labels,
		})
	}

	m := &Metrics{
		registry: registry,

		HTTPRequestCount: counter("http_requests_total",
			"Total HTTP requests.", "method", "route", "status"),
		HTTPRequestLatency: histogram("http_request_duration_seconds",
			"HTTP request latency.", prometheus.DefBuckets, "method", "route"),
		HTTPErrorCount: counter("http_errors_total",
			"Total HTTP responses with status >= 400.", "method", "route", "status"),

		GRPCRequestCount: counter("grpc_requests_total",
			"Total gRPC requests.", "method", "code"),
		GRPCRequestLatency: histogram("grpc_request_duration_seconds",
			"gRPC request latency.", prometheus.DefBuckets, "method"),

		KafkaProcessedCount: counter("kafka_messages_processed_total",
			"Successfully processed Kafka messages.", "consumer", "event_type"),
		KafkaSkippedCount: counter("kafka_messages_skipped_total",
			"Kafka messages skipped without business effect.", "consumer", "event_type", "reason"),
		KafkaErrorCount: counter("kafka_consumer_errors_total",
			"Kafka consumer errors.", "consumer", "event_type"),
		KafkaDLQCount: counter("kafka_dlq_messages_total",
			"Messages routed to the dead letter queue.", "consumer", "reason"),
		KafkaProcessingDuration: histogram("kafka_processing_duration_seconds",
			"Kafka message handling latency.",
			[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			"consumer", "event_type"),

		OrdersCreated: counter("orders_created_total",
			"Orders created through the public API.", "result"),
		OrderStatusApplied: counter("order_status_transitions_total",
			"Applied order status transitions.", "from", "to"),
		PaymentsTotal: counter("payments_total",
			"Payment attempts by outcome.", "status"),
		ReservationsTotal: counter("inventory_reservations_total",
			"Inventory reservation outcomes.", "status"),
		ProductionTasks: counter("production_tasks_total",
			"Production task lifecycle events.", "status"),
		OutboxPublished: counter("outbox_events_published_total",
			"Outbox events published to Kafka.", "topic", "result"),

		OutboxPendingTotal: gauge("outbox_events_pending",
			"Outbox events awaiting publication."),
		OutboxOldestSeconds: gauge("outbox_oldest_pending_age_seconds",
			"Age of the oldest unpublished outbox event."),
	}

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequestCount, m.HTTPRequestLatency, m.HTTPErrorCount,
		m.GRPCRequestCount, m.GRPCRequestLatency,
		m.KafkaProcessedCount, m.KafkaSkippedCount, m.KafkaErrorCount,
		m.KafkaDLQCount, m.KafkaProcessingDuration,
		m.OrdersCreated, m.OrderStatusApplied, m.PaymentsTotal,
		m.ReservationsTotal, m.ProductionTasks,
		m.OutboxPublished, m.OutboxPendingTotal, m.OutboxOldestSeconds,
	)

	return m
}

// Register позволяет добавить в реестр сервисные коллекторы (например, пул БД).
func (m *Metrics) Register(collector prometheus.Collector) error {
	return m.registry.Register(collector)
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// HTTPMiddleware считает запросы, латентность и ошибки.
//
// Важно: в качестве лейбла используется шаблон маршрута ("/api/orders/{id}"),
// а не сырой путь. Иначе каждый order_id порождает отдельную time series
// и Prometheus захлёбывается кардинальностью.
func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &metricStatusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(recorder, r)

		route := appctx.Route(r.Context(), "unmatched")
		status := strconv.Itoa(recorder.status)

		m.HTTPRequestCount.WithLabelValues(r.Method, route, status).Inc()
		m.HTTPRequestLatency.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
		if recorder.status >= http.StatusBadRequest {
			m.HTTPErrorCount.WithLabelValues(r.Method, route, status).Inc()
		}
	})
}

type metricStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *metricStatusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Flush и Unwrap сохраняют совместимость с обёрнутым ResponseWriter.
func (r *metricStatusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
