package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/operation/mocks"
	storagemocks "db-worker/internal/storage/mocks"
	"db-worker/internal/storage/model"
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
		createOpts func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option
		wantErr    require.ErrorAssertionFunc
		createWant func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) *Service
	}{
		{
			name: "positive case",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMsgChan(msgChan),
					WithUow(uow),
					WithMessageRepo(messageRepo),
					WithDriversMap(driversMap),
					WithInstanceID(1),
				}
			},
			createWant: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) *Service {
				t.Helper()

				return &Service{
					cfg:         &operation.Operation{},
					msgChan:     msgChan,
					quitChan:    make(chan struct{}),
					uow:         uow,
					messageRepo: messageRepo,
					driversMap:  driversMap,
					instanceID:  1,
				}
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: cfg is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option {
				t.Helper()

				return []option{
					WithMsgChan(msgChan),
					WithUow(uow),
					WithMsgChan(msgChan),
					WithMessageRepo(messageRepo),
					WithDriversMap(driversMap),
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message channel is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMessageRepo(messageRepo),
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: uow is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMsgChan(msgChan),
					WithMessageRepo(messageRepo),
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message repo is nil",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMsgChan(msgChan),
					WithUow(uow),
					WithDriversMap(driversMap),
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: drivers map is empty",
			createOpts: func(t *testing.T, uow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{}),
					WithMsgChan(msgChan),
					WithUow(uow),
					WithMessageRepo(messageRepo),
					WithInstanceID(1),
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
			messageRepo := mocks.NewMockmessageRepo(ctrl)

			configurator1 := storagemocks.NewMockConfigurator(ctrl)
			configurator2 := storagemocks.NewMockConfigurator(ctrl)

			driversMap := map[string]model.Configurator{
				"test-storage":   configurator1,
				"test-storage-2": configurator2,
			}

			got, err := New(tt.createOpts(t, uow, messageRepo, driversMap)...)
			tt.wantErr(t, err)

			if tt.createWant != nil {
				want := tt.createWant(t, uow, messageRepo, driversMap)
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
