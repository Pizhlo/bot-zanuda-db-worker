package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Service предоставляет методы для записи бизнес-метрик в Prometheus.
type Service struct {
	registry  prometheus.Registerer
	namespace string
	subsystem string
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

	return s
}
