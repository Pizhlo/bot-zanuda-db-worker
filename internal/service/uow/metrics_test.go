package uow

import (
	"db-worker/internal/service/uow/mocks"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestAddTotalTransactions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMocktxCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddTotalTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addTotalTransactions(1)
}

func TestAddInProgressTransactions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMocktxCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddInProgressTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addInProgressTransactions(1)
}

//nolint:dupl // схожие тест-кейсы
func TestAddFailedTransactions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMocktxCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddFailedTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	metricsService.EXPECT().DecrementInProgressTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addFailedTransactions(1)
}

//nolint:dupl // схожие тест-кейсы
func TestAddCanceledTransactions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMocktxCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddCanceledTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	metricsService.EXPECT().DecrementInProgressTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addCanceledTransactions(1)
}

//nolint:dupl // схожие тест-кейсы
func TestAddSuccessTransactions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsService := mocks.NewMocktxCounter(ctrl)

	srv := &Service{
		metricsService: metricsService,
	}

	metricsService.EXPECT().AddSuccessTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	metricsService.EXPECT().DecrementInProgressTransactions(gomock.Any()).Return().Times(1).Do(func(count int) {
		assert.Equal(t, 1, count)
	})

	srv.addSuccessTransactions(1)
}
