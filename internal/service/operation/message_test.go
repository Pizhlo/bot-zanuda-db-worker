package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/operation/mocks"
	"db-worker/internal/service/uow"
	storagemocks "db-worker/internal/storage/mocks"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestReadMessages(t *testing.T) {
	t.Parallel()

	msgChan := make(chan map[string]interface{})
	quitChan := make(chan struct{})

	tests := []struct {
		name string
		svc  *Service
		msg  map[string]interface{}
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
				msgChan:  msgChan,
				quitChan: quitChan,
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockUow := mocks.NewMockunitOfWork(ctrl)

			// AnyTimes - потому что мы не знаем, в какой момент будет закрыть канал
			mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).AnyTimes()
			mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockUow.EXPECT().ExecRequests(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			tt.svc.uow = mockUow

			go tt.svc.readMessages(ctx)

			tt.svc.msgChan <- tt.msg

			close(tt.svc.quitChan)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestProcessMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		svc        *Service
		msg        map[string]interface{}
		setupMocks func(t *testing.T, mockUow *mocks.MockunitOfWork, mockDriver *storagemocks.MockDriver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, mockDriver *storagemocks.MockDriver) {
				t.Helper()

				mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).Times(1)
				mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockUow.EXPECT().ExecRequests(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: error validate message",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
						{
							Name:     "field2",
							Type:     "string",
							Required: true,
						},
					},
				},
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, mockDriver *storagemocks.MockDriver) {
				t.Helper()
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: error build requests",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, mockDriver *storagemocks.MockDriver) {
				t.Helper()

				mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).Times(1)
				mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("error")).Times(1)
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: error exec requests",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, mockDriver *storagemocks.MockDriver) {
				t.Helper()

				mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).Times(1)
				mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockUow.EXPECT().ExecRequests(gomock.Any(), gomock.Any()).Return(errors.New("error")).Times(1)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockUow := mocks.NewMockunitOfWork(ctrl)
			mockDriver := storagemocks.NewMockDriver(ctrl)

			tt.setupMocks(t, mockUow, mockDriver)

			tt.svc.uow = mockUow

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			err := tt.svc.processMessage(ctx, tt.msg)
			tt.wantErr(t, err)
		})
	}
}
