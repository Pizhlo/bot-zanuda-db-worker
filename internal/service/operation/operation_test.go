package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/operation/mocks"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestNew(t *testing.T) {
	t.Parallel()

	msgChan := make(chan map[string]interface{})

	tests := []struct {
		name       string
		createOpts func(t *testing.T, uow *mocks.MockunitOfWork) []option
		wantErr    require.ErrorAssertionFunc
		createWant func(t *testing.T, uow *mocks.MockunitOfWork) *Service
	}{
		{
			name: "positive case",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMsgChan(msgChan),
					WithUow(uow),
				}
			},
			createWant: func(t *testing.T, uow *mocks.MockunitOfWork) *Service {
				t.Helper()

				return &Service{
					cfg:      &operation.Operation{},
					msgChan:  msgChan,
					quitChan: make(chan struct{}),
					uow:      uow,
				}
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: cfg is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork) []option {
				t.Helper()

				return []option{
					WithMsgChan(msgChan),
					WithUow(uow),
					WithMsgChan(msgChan),
				}
			},
			createWant: func(t *testing.T, uow *mocks.MockunitOfWork) *Service {
				t.Helper()

				return &Service{
					cfg:      &operation.Operation{},
					msgChan:  msgChan,
					quitChan: make(chan struct{}),
					uow:      uow,
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message channel is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: uow is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMsgChan(msgChan),
				}
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			uow := mocks.NewMockunitOfWork(ctrl)

			got, err := New(tt.createOpts(t, uow)...)
			tt.wantErr(t, err)

			if got != nil {
				want := tt.createWant(t, uow)
				// Сравниваем поля по отдельности, исключая каналы
				assert.Equal(t, want.cfg, got.cfg)
				assert.Equal(t, want.msgChan, got.msgChan)
				assert.Equal(t, want.uow, got.uow)
				assert.NotNil(t, got.quitChan)
			}
		})
	}
}

func TestStop(t *testing.T) {
	t.Parallel()

	op := &Service{
		quitChan: make(chan struct{}),
		cfg: &operation.Operation{
			Name: "test",
		},
	}

	require.NoError(t, op.Stop(context.Background()))

	_, ok := <-op.quitChan
	assert.False(t, ok)
}
