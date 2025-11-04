package metrics

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestAddProcessingMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// во избежание конфликта имен
	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddProcessingMessages(1)

	value := testutil.ToFloat64(metricsService.processingMessages)
	assert.Equal(t, float64(1), value)
}

func TestAddFailedMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddFailedMessages(1)

	value := testutil.ToFloat64(metricsService.failedMessages)
	assert.Equal(t, float64(1), value)
}

func TestAddValidatedMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddValidatedMessages(1)

	value := testutil.ToFloat64(metricsService.validatedMessages)
	assert.Equal(t, float64(1), value)
}

func TestAddProcessedMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddProcessedMessages(1)

	value := testutil.ToFloat64(metricsService.processedMessages)
	assert.Equal(t, float64(1), value)
}

func TestAddTotalMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddTotalMessages(1)

	value := testutil.ToFloat64(metricsService.totalMessages)
	assert.Equal(t, float64(1), value)
}

func TestDecrementProcessingMessagesBy(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddProcessingMessages(20)
	metricsService.DecrementProcessingMessagesBy(1)

	value := testutil.ToFloat64(metricsService.processingMessages)
	assert.Equal(t, float64(19), value)
}

func TestDecrementFailedMessagesBy(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registry := prometheus.NewRegistry()
	metricsService := New(WithRegisterer(registry))

	metricsService.AddFailedMessages(20)
	metricsService.DecrementFailedMessagesBy(1)

	value := testutil.ToFloat64(metricsService.failedMessages)
	assert.Equal(t, float64(19), value)
}
