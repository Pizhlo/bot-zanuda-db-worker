package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Service предоставляет методы для записи бизнес-метрик в Prometheus.
type Service struct {
	registry  prometheus.Registerer
	namespace string
	subsystem string

	// Метрики для сообщений
	processingMessages prometheus.Gauge // количество сообщений в процессе обработки
	failedMessages     prometheus.Gauge // количество сообщений в статусе failed
	validatedMessages  prometheus.Gauge // количество сообщений в статусе validated
	processedMessages  prometheus.Gauge // количество обработанных сообщений
	totalMessages      prometheus.Gauge // общее количество сообщений

	// Метрики для транзакций
	totalTransactions      prometheus.Gauge // общее количество транзакций
	inProgressTransactions prometheus.Gauge // количество транзакций в статусе in progress
	failedTransactions     prometheus.Gauge // количество транзакций в статусе failed
	canceledTransactions   prometheus.Gauge // количество транзакций в статусе canceled
	successTransactions    prometheus.Gauge // количество транзакций в статусе success
}

// Option описывает опции инициализации сервиса метрик.
type Option func(*options)

type options struct {
	registry  prometheus.Registerer
	namespace string
	subsystem string
}

// WithRegisterer позволяет передать кастомный регистратор (иначе используется DefaultRegisterer).
func WithRegisterer(r prometheus.Registerer) Option {
	return func(o *options) { o.registry = r }
}

// WithNamespace задаёт namespace для метрик.
func WithNamespace(namespace string) Option {
	return func(o *options) { o.namespace = namespace }
}

// WithSubsystem задаёт subsystem для метрик.
func WithSubsystem(subsystem string) Option {
	return func(o *options) { o.subsystem = subsystem }
}

// New создаёт сервис метрик.
func New(opts ...Option) *Service {
	o := &options{
		registry:  prometheus.DefaultRegisterer,
		namespace: "dbworker",
		subsystem: "core",
	}

	for _, opt := range opts {
		opt(o)
	}

	s := &Service{
		registry:  o.registry,
		namespace: o.namespace,
		subsystem: o.subsystem,
	}

	logrus.WithFields(logrus.Fields{
		"namespace": s.namespace,
		"subsystem": s.subsystem,
	}).Info("metrics: service created")

	s.registerMetrics()

	logrus.WithFields(logrus.Fields{
		"namespace": s.namespace,
		"subsystem": s.subsystem,
	}).Info("metrics: metrics registered")

	return s
}

func (s *Service) registerMetrics() {
	s.registerMessageMetrics()
	s.registerTransactionMetrics()
}

//nolint:dupl // похожая реализация, с разницей в устанавливаемых метриках
func (s *Service) registerMessageMetrics() {
	s.processingMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "processing_messages_total",
			Help:      "Total number of processing messages",
		},
	)
	s.registry.MustRegister(s.processingMessages)

	s.failedMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "failed_messages_total",
			Help:      "Total number of failed messages",
		},
	)
	s.registry.MustRegister(s.failedMessages)

	s.validatedMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "validated_messages_total",
			Help:      "Total number of validated messages",
		},
	)
	s.registry.MustRegister(s.validatedMessages)

	s.processedMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "processed_messages_total",
			Help:      "Total number of processed messages",
		},
	)
	s.registry.MustRegister(s.processedMessages)

	s.totalMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "total_messages",
			Help:      "Total number of messages",
		},
	)
	s.registry.MustRegister(s.totalMessages)
}

//nolint:dupl // похожая реализация, с разницей в устанавливаемых метриках
func (s *Service) registerTransactionMetrics() {
	s.totalTransactions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "transactions_total",
			Help:      "Total number of transactions",
		},
	)
	s.registry.MustRegister(s.totalTransactions)

	s.inProgressTransactions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "in_progress_transactions_total",
			Help:      "Total number of in progress transactions",
		},
	)
	s.registry.MustRegister(s.inProgressTransactions)

	s.failedTransactions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "failed_transactions_total",
			Help:      "Total number of failed transactions",
		},
	)
	s.registry.MustRegister(s.failedTransactions)

	s.canceledTransactions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "canceled_transactions_total",
			Help:      "Total number of canceled transactions",
		},
	)
	s.registry.MustRegister(s.canceledTransactions)

	s.successTransactions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "success_transactions_total",
			Help:      "Total number of success transactions",
		},
	)
	s.registry.MustRegister(s.successTransactions)
}
