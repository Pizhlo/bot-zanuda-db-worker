package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestAddTotalTransactions(t *testing.T) {
	t.Parallel()

	// во избежание конфликта имен
	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddTotalTransactions(1)

	value := testutil.ToFloat64(metricsService.totalTransactions)
	assert.Equal(t, float64(1), value)
}

func TestAddInProgressTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddInProgressTransactions(1)

	value := testutil.ToFloat64(metricsService.inProgressTransactions)
	assert.Equal(t, float64(1), value)
}

func TestAddFailedTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddFailedTransactions(1)

	value := testutil.ToFloat64(metricsService.failedTransactions)
	assert.Equal(t, float64(1), value)
}

func TestAddCanceledTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddCanceledTransactions(1)

	value := testutil.ToFloat64(metricsService.canceledTransactions)
	assert.Equal(t, float64(1), value)
}

func TestAddSuccessTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddSuccessTransactions(1)

	value := testutil.ToFloat64(metricsService.successTransactions)
	assert.Equal(t, float64(1), value)
}

func TestDecrementTotalTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddTotalTransactions(1)
	metricsService.DecrementTotalTransactions(1)

	value := testutil.ToFloat64(metricsService.totalTransactions)
	assert.Equal(t, float64(0), value)
}

func TestDecrementInProgressTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddInProgressTransactions(1)
	metricsService.DecrementInProgressTransactions(1)

	value := testutil.ToFloat64(metricsService.inProgressTransactions)
	assert.Equal(t, float64(0), value)
}

func TestDecrementFailedTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddFailedTransactions(1)
	metricsService.DecrementFailedTransactions(1)

	value := testutil.ToFloat64(metricsService.failedTransactions)
	assert.Equal(t, float64(0), value)
}

func TestDecrementCanceledTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddCanceledTransactions(1)
	metricsService.DecrementCanceledTransactions(1)

	value := testutil.ToFloat64(metricsService.canceledTransactions)
	assert.Equal(t, float64(0), value)
}

func TestDecrementSuccessTransactions(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddSuccessTransactions(1)
	metricsService.DecrementSuccessTransactions(1)

	value := testutil.ToFloat64(metricsService.successTransactions)
	assert.Equal(t, float64(0), value)
}
