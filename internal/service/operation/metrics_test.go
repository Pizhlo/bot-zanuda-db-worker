package operation

import (
	"db-worker/internal/service/operation/mocks"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

//nolint:dupl // похожие тесты
func TestAddFailedMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMockmessageCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddFailedMessages(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addFailedMessages(1)
}

//nolint:dupl // похожие тесты
func TestAddValidatedMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMockmessageCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addValidatedMessages(1)
}

func TestAddProcessingMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMockmessageCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addProcessingMessages(1)
}

//nolint:dupl // похожие тесты
func TestFromFailedToValidated(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMockmessageCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	metricsService.EXPECT().DecrementFailedMessagesBy(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.fromFailedToValidated(1)
}

func TestAddTotalMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMockmessageCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddTotalMessages(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addTotalMessages(1)
}

func TestAddProcessedMessages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMockmessageCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddProcessedMessages(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addProcessedMessages(1)
}
