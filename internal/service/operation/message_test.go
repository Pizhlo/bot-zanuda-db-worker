package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/operation/message"
	"db-worker/internal/service/operation/mocks"
	"db-worker/internal/service/uow"
	storagemocks "db-worker/internal/storage/mocks"
	"db-worker/internal/storage/model"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // много тест-кейсов
func TestReadMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  map[string]interface{}
	}{
		{
			name: "positive case",
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

			messageRepo := mocks.NewMockmessageRepo(ctrl)

			metricsService := mocks.NewMockmessageCounter(ctrl)

			configurator1 := storagemocks.NewMockConfigurator(ctrl)
			configurator2 := storagemocks.NewMockConfigurator(ctrl)

			driversMap := map[string]model.Configurator{
				"test-storage":   configurator1,
				"test-storage-2": configurator2,
			}

			svc := &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
				messageRepo:    messageRepo,
				uow:            mockUow,
				driversMap:     driversMap,
				instanceID:     1,
				metricsService: metricsService,
				messages:       make(map[uuid.UUID]*message.Message),
				msgChan:        make(chan map[string]interface{}),
				quitChan:       make(chan struct{}),
			}

			// AnyTimes - потому что мы не знаем, в какой момент будет закрыть канал
			mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).AnyTimes()
			mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockUow.EXPECT().ExecRequests(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			configurator1.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
			configurator1.EXPECT().Name().Return("test-storage").AnyTimes()
			configurator2.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
			configurator2.EXPECT().Name().Return("test-storage-2").AnyTimes()

			messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).AnyTimes().Do(func(ctx context.Context, messages []message.Message) error {
				for _, msg := range messages {
					assert.Equal(t, message.StatusInProgress, msg.Status)
				}

				return nil
			})

			messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).AnyTimes().Do(func(ctx context.Context, messages []message.Message) error {
				for _, msg := range messages {
					assert.Equal(t, message.StatusValidated, msg.Status)
				}

				return nil
			})

			metricsService.EXPECT().AddTotalMessages(gomock.Any()).Return().AnyTimes()
			metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().AnyTimes()
			metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().AnyTimes()
			metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().AnyTimes()
			metricsService.EXPECT().AddProcessedMessages(gomock.Any()).Return().AnyTimes()

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()

				svc.readMessages(t.Context())
			}()

			svc.msgChan <- tt.msg

			// Даем время на обработку сообщения перед закрытием канала
			time.Sleep(10 * time.Millisecond)

			close(svc.quitChan)

			// Ждем завершения горутины, чтобы все defer функции успели выполниться
			wg.Wait()
		})
	}
}

//nolint:funlen,gocognit,cyclop // много тест-кейсов, сложный тест - ок
func TestProcessMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service
		msg        map[string]interface{}
		setupMocks func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test",
					},
					messageRepo:    messageRepo,
					uow:            mockUow,
					driversMap:     driversMap,
					instanceID:     1,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).Times(1)
				mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockUow.EXPECT().ExecRequests(gomock.Any(), gomock.Any()).Return(nil).Times(1)

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusInProgress, msg.Status)
					}

					return nil
				})

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusValidated, msg.Status)
					}

					return nil
				})

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
					driver.EXPECT().Name().Return("test-storage").Times(1)
				}

				metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddProcessedMessages(gomock.Any()).Return().AnyTimes()
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: error validate message",
			createSvc: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
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
					messageRepo:    messageRepo,
					uow:            mockUow,
					driversMap:     driversMap,
					instanceID:     1,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusInProgress, msg.Status)
					}

					return nil
				})

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusFailed, msg.Status)
						assert.Equal(t, "operation: error validate fields: field \"field2\" is required", msg.Error)
					}

					return nil
				})

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
					driver.EXPECT().Name().Return("test-storage").Times(1)
				}

				metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddFailedMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddProcessedMessages(gomock.Any()).Return().AnyTimes()
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: error build requests",
			createSvc: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test",
					},
					messageRepo:    messageRepo,
					uow:            mockUow,
					driversMap:     driversMap,
					instanceID:     1,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusInProgress, msg.Status)
					}

					return nil
				})

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusFailed, msg.Status)
						assert.Equal(t, "error", msg.Error)
					}

					return nil
				})

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
					driver.EXPECT().Name().Return("test-storage").Times(1)
				}

				mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).Times(1)
				mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("error")).Times(1)

				metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddFailedMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddProcessedMessages(gomock.Any()).Return().AnyTimes()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: error exec requests",
			createSvc: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test",
					},
					messageRepo:    messageRepo,
					uow:            mockUow,
					driversMap:     driversMap,
					instanceID:     1,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			setupMocks: func(t *testing.T, mockUow *mocks.MockunitOfWork, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				mockUow.EXPECT().StoragesMap().Return(map[string]uow.DriversMap{}).Times(1)
				mockUow.EXPECT().BuildRequests(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockUow.EXPECT().ExecRequests(gomock.Any(), gomock.Any()).Return(errors.New("error")).Times(1)

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusInProgress, msg.Status)
					}

					return nil
				})

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					for _, msg := range messages {
						assert.Equal(t, message.StatusValidated, msg.Status)
					}

					return nil
				})

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
					driver.EXPECT().Name().Return("test-storage").Times(1)
				}

				metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().AnyTimes()
				metricsService.EXPECT().AddProcessedMessages(gomock.Any()).Return().AnyTimes()
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
			messageRepo := mocks.NewMockmessageRepo(ctrl)

			configurator1 := storagemocks.NewMockConfigurator(ctrl)
			configurator2 := storagemocks.NewMockConfigurator(ctrl)

			metricsService := mocks.NewMockmessageCounter(ctrl)

			driversMap := map[string]model.Configurator{
				"test-storage":   configurator1,
				"test-storage-2": configurator2,
			}

			tt.setupMocks(t, mockUow, messageRepo, []*storagemocks.MockConfigurator{configurator1, configurator2}, metricsService)

			svc := tt.createSvc(t, mockUow, messageRepo, driversMap, metricsService)

			// сохраняем сообщения в память
			ids, err := svc.createMessages(t.Context(), tt.msg)
			require.NoError(t, err)

			err = svc.processMessage(t.Context(), tt.msg, ids)
			tt.wantErr(t, err)

			assert.Len(t, svc.messages, 0)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestCreateMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service
		msg        map[string]interface{}
		driversMap func(t *testing.T, drivers []model.Configurator) map[string]model.Configurator
		setupMocks func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter)
		wantErr    require.ErrorAssertionFunc
		validate   func(t *testing.T, svc *Service, ids []uuid.UUID)
	}{
		{
			name: "positive case: create messages for all drivers",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test",
						Hash: []byte{1, 2, 3, 4},
					},
					messageRepo:    messageRepo,
					driversMap:     driversMap,
					instanceID:     42,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			msg: map[string]interface{}{
				"field1": "value1",
				"field2": 123,
			},
			driversMap: func(t *testing.T, drivers []model.Configurator) map[string]model.Configurator {
				t.Helper()

				return map[string]model.Configurator{
					"test-storage":   drivers[0],
					"test-storage-2": drivers[1],
				}
			},
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
					driver.EXPECT().Name().Return("test-storage").AnyTimes()
				}

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					require.Len(t, messages, len(driversMap))

					for _, msg := range messages {
						assert.Equal(t, message.StatusInProgress, msg.Status)
						assert.Equal(t, map[string]interface{}{
							"field1": "value1",
							"field2": 123,
						}, msg.Data)
						assert.Equal(t, 42, msg.InstanceID)
						assert.Equal(t, []byte{1, 2, 3, 4}, msg.OperationHash)

						assert.NotEqual(t, uuid.Nil, msg.ID)
						assert.NotEmpty(t, msg.DriverType)
						assert.NotEmpty(t, msg.DriverName)
					}

					return nil
				})

				metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().Times(1)
			},
			wantErr: require.NoError,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID) {
				t.Helper()

				require.Len(t, ids, 2)
				assert.Len(t, svc.messages, 2)

				for _, id := range ids {
					assert.NotEqual(t, uuid.Nil, id)

					msg, ok := svc.messages[id]
					require.True(t, ok, "message should be in map")

					assert.Equal(t, message.StatusInProgress, msg.Status)
					assert.Equal(t, map[string]interface{}{
						"field1": "value1",
						"field2": 123,
					}, msg.Data)
					assert.Equal(t, 42, msg.InstanceID)
					assert.Equal(t, []byte{1, 2, 3, 4}, msg.OperationHash)
				}
			},
		},
		{
			name: "positive case: single driver",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test",
						Hash: []byte{5, 6, 7, 8},
					},
					messageRepo:    messageRepo,
					driversMap:     driversMap,
					instanceID:     100,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			driversMap: func(t *testing.T, drivers []model.Configurator) map[string]model.Configurator {
				t.Helper()

				return map[string]model.Configurator{
					"test-storage": drivers[0],
				}
			},
			msg: map[string]interface{}{
				"test": "data",
			},
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
					driver.EXPECT().Name().Return("test-storage").AnyTimes()
				}

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, messages []message.Message) error {
					require.Len(t, messages, 1)

					msg := messages[0]
					assert.Equal(t, message.StatusInProgress, msg.Status)
					assert.Equal(t, map[string]interface{}{"test": "data"}, msg.Data)
					assert.Equal(t, 100, msg.InstanceID)
					assert.Equal(t, []byte{5, 6, 7, 8}, msg.OperationHash)

					return nil
				})

				metricsService.EXPECT().AddProcessingMessages(gomock.Any()).Return().Times(1)
			},
			wantErr: require.NoError,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID) {
				t.Helper()

				require.Len(t, ids, 1)
				assert.Len(t, svc.messages, 1)
			},
		},
		{
			name: "negative case: error create many",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap map[string]model.Configurator, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test",
						Hash: []byte{1, 2, 3},
					},
					messageRepo: messageRepo,
					driversMap:  driversMap,
					instanceID:  50,
					messages:    make(map[uuid.UUID]*message.Message),
				}
			},
			driversMap: func(t *testing.T, drivers []model.Configurator) map[string]model.Configurator {
				t.Helper()

				return map[string]model.Configurator{
					"test-storage":   drivers[0],
					"test-storage-2": drivers[1],
				}
			},
			msg: map[string]interface{}{
				"field": "value",
			},
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, driversMap []*storagemocks.MockConfigurator, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				for _, driver := range driversMap {
					driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
					driver.EXPECT().Name().Return("test-storage").AnyTimes()
				}

				messageRepo.EXPECT().CreateMany(gomock.Any(), gomock.Any()).Return(errors.New("database error")).Times(1)
			},
			wantErr: require.Error,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID) {
				t.Helper()

				// Сообщения все равно добавляются в карту перед вызовом CreateMany
				assert.Len(t, svc.messages, len(svc.driversMap))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			messageRepo := mocks.NewMockmessageRepo(ctrl)

			configurator1 := storagemocks.NewMockConfigurator(ctrl)
			configurator2 := storagemocks.NewMockConfigurator(ctrl)

			metricsService := mocks.NewMockmessageCounter(ctrl)

			driversMap := tt.driversMap(t, []model.Configurator{configurator1, configurator2})

			tt.setupMocks(t, messageRepo, []*storagemocks.MockConfigurator{configurator1, configurator2}, metricsService)

			svc := tt.createSvc(t, messageRepo, driversMap, metricsService)

			ids, err := svc.createMessages(t.Context(), tt.msg)
			tt.wantErr(t, err)

			if tt.validate != nil {
				tt.validate(t, svc, ids)
			}
		})
	}
}

//nolint:funlen,gocognit,dupl,cyclop // много тест-кейсов, сложный тест - ок, дублируются настройки моков
func TestUpdateMessagesStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		createSvc       func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service
		setupMocks      func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter)
		prepareMessages func(t *testing.T, svc *Service) []uuid.UUID
		status          message.Status
		errMsg          error
		wantErr         require.ErrorAssertionFunc
		validate        func(t *testing.T, svc *Service, ids []uuid.UUID, status message.Status, errMsg error)
	}{
		{
			name: "positive case: update status to validated without error",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo:    messageRepo,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusValidated,
			errMsg: nil,
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, msgs []message.Message) error {
					require.Len(t, msgs, 1)
					assert.Equal(t, message.StatusValidated, msgs[0].Status)
					assert.Empty(t, msgs[0].Error)

					return nil
				})

				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1)
			},
			wantErr: require.NoError,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID, status message.Status, errMsg error) {
				t.Helper()

				for _, id := range ids {
					msg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)

					assert.Equal(t, status, msg.Status)

					if errMsg != nil {
						assert.Equal(t, errMsg.Error(), msg.Error)
					} else {
						assert.Empty(t, msg.Error)
					}
				}
			},
		},
		{
			name: "positive case: update status to failed with error",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo:    messageRepo,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusFailed,
			errMsg: errors.New("validation error: field required"),
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, msgs []message.Message) error {
					require.Len(t, msgs, 1)
					assert.Equal(t, message.StatusFailed, msgs[0].Status)
					assert.Equal(t, "validation error: field required", msgs[0].Error)

					return nil
				})

				metricsService.EXPECT().AddFailedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1)
			},
			wantErr: require.NoError,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID, status message.Status, errMsg error) {
				t.Helper()

				for _, id := range ids {
					msg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)

					assert.Equal(t, status, msg.Status)

					if errMsg != nil {
						assert.Equal(t, errMsg.Error(), msg.Error)
					} else {
						assert.Empty(t, msg.Error)
					}
				}
			},
		},
		{
			name: "positive case: update multiple messages",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo:    messageRepo,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				msg2 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field2": "value2"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage-2",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)
				svc.addMessageToMap(msg2.ID, msg2)

				return []uuid.UUID{msg1.ID, msg2.ID}
			},
			status: message.StatusValidated,
			errMsg: nil,
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, msgs []message.Message) error {
					require.Len(t, msgs, 2)

					for _, msg := range msgs {
						assert.Equal(t, message.StatusValidated, msg.Status)
						assert.Empty(t, msg.Error)
					}

					return nil
				})

				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(2)
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(2)
			},
			wantErr: require.NoError,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID, status message.Status, errMsg error) {
				t.Helper()

				require.Len(t, ids, 2)

				for _, id := range ids {
					msg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)

					assert.Equal(t, status, msg.Status)

					if errMsg != nil {
						assert.Equal(t, errMsg.Error(), msg.Error)
					} else {
						assert.Empty(t, msg.Error)
					}
				}
			},
		},
		{
			name: "negative case: message not found in map",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo:    messageRepo,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				// Возвращаем UUID, который не существует в карте
				return []uuid.UUID{uuid.New()}
			},
			status:     message.StatusFailed,
			errMsg:     errors.New("test error"),
			setupMocks: nil, // Не вызываем setupMocks, т.к. сообщение не найдено в карте
			wantErr:    require.Error,
			validate:   nil,
		},
		{
			name: "negative case: error update messages in database",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo:    messageRepo,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusValidated,
			errMsg: nil,
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(errors.New("database connection error")).Times(1)

				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1)
			},
			wantErr: require.Error,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID, status message.Status, errMsg error) {
				t.Helper()

				// Проверяем, что статус в карте все равно обновился (хотя обновление в БД не прошло)
				for _, id := range ids {
					msg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)
					assert.Equal(t, status, msg.Status)
				}
			},
		},
		{
			name: "negative case: errMsg is nil but status is failed",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo: messageRepo,
					messages:    make(map[uuid.UUID]*message.Message),
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusFailed,
			errMsg: nil,
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				// UpdateMany не должен быть вызван, т.к. валидация вернет ошибку до этого
				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Times(0)
			},
			wantErr: func(t require.TestingT, err error, _ ...interface{}) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "err == nil, but status is failed")
			},
			validate: nil,
		},
		{
			name: "negative case: errMsg is not nil but status is not failed",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo: messageRepo,
					messages:    make(map[uuid.UUID]*message.Message),
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusValidated,
			errMsg: errors.New("some error"),
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				// UpdateMany не должен быть вызван, т.к. валидация вернет ошибку до этого
				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Times(0)
			},
			wantErr: func(t require.TestingT, err error, _ ...interface{}) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "err != nil, but status is not failed")
				require.Contains(t, err.Error(), "VALIDATED")
			},
			validate: nil,
		},
		{
			name: "negative case: errMsg is not nil but status is in progress",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo: messageRepo,
					messages:    make(map[uuid.UUID]*message.Message),
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusInProgress,
			errMsg: errors.New("some error"),
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				// UpdateMany не должен быть вызван, т.к. валидация вернет ошибку до этого
				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Times(0)
			},
			wantErr: func(t require.TestingT, err error, _ ...interface{}) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "err != nil, but status is not failed")
				require.Contains(t, err.Error(), "IN_PROGRESS")
			},
			validate: nil,
		},
		{
			name: "positive case: update status from failed to validated",
			createSvc: func(t *testing.T, messageRepo *mocks.MockmessageRepo, metricsService *mocks.MockmessageCounter) *Service {
				t.Helper()

				return &Service{
					messageRepo:    messageRepo,
					messages:       make(map[uuid.UUID]*message.Message),
					metricsService: metricsService,
				}
			},
			prepareMessages: func(t *testing.T, svc *Service) []uuid.UUID {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusFailed,
					Error:         "previous error",
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg1.ID, msg1)

				return []uuid.UUID{msg1.ID}
			},
			status: message.StatusValidated,
			errMsg: nil,
			setupMocks: func(t *testing.T, messageRepo *mocks.MockmessageRepo, messages []message.Message, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				messageRepo.EXPECT().UpdateMany(gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, msgs []message.Message) error {
					require.Len(t, msgs, 1)

					assert.Equal(t, message.StatusValidated, msgs[0].Status)
					assert.Empty(t, msgs[0].Error)

					return nil
				})

				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementFailedMessagesBy(gomock.Any()).Return().Times(1)
			},
			wantErr: require.NoError,
			validate: func(t *testing.T, svc *Service, ids []uuid.UUID, status message.Status, errMsg error) {
				t.Helper()

				for _, id := range ids {
					msg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)
					assert.Equal(t, status, msg.Status)
					assert.Empty(t, msg.Error)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			messageRepo := mocks.NewMockmessageRepo(ctrl)
			metricsService := mocks.NewMockmessageCounter(ctrl)

			svc := tt.createSvc(t, messageRepo, metricsService)

			ids := tt.prepareMessages(t, svc)

			if tt.setupMocks != nil {
				var messages []message.Message

				for _, id := range ids {
					msg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)

					messages = append(messages, *msg)
				}

				tt.setupMocks(t, messageRepo, messages, metricsService)
			}

			err := svc.updateMessagesStatus(t.Context(), tt.status, ids, tt.errMsg)
			tt.wantErr(t, err)

			if tt.validate != nil {
				tt.validate(t, svc, ids, tt.status, tt.errMsg)
			}
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestDeleteMessagesFromMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		prepareMessages func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID)
		deleteIDs       []uuid.UUID
		validate        func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message)
	}{
		{
			name: "positive case: delete single message",
			prepareMessages: func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID) {
				t.Helper()

				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg.ID, msg)

				original := map[uuid.UUID]*message.Message{
					msg.ID: msg,
				}

				return original, []uuid.UUID{msg.ID}
			},
			deleteIDs: nil, // будет заполнено из prepareMessages
			validate: func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message) {
				t.Helper()

				for id := range originalMessages {
					_, err := svc.getMessageFromMap(id)
					require.Error(t, err)
					require.Contains(t, err.Error(), "message not found by id")
				}
			},
		},
		{
			name: "positive case: delete multiple messages",
			prepareMessages: func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID) {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				msg2 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field2": "value2"},
					Status:        message.StatusInProgress,
					DriverType:    "redis",
					DriverName:    "test-storage-2",
					InstanceID:    1,
					OperationHash: []byte{4, 5, 6},
				}
				msg3 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field3": "value3"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage-3",
					InstanceID:    1,
					OperationHash: []byte{7, 8, 9},
				}

				svc.addMessageToMap(msg1.ID, msg1)
				svc.addMessageToMap(msg2.ID, msg2)
				svc.addMessageToMap(msg3.ID, msg3)

				original := map[uuid.UUID]*message.Message{
					msg1.ID: msg1,
					msg2.ID: msg2,
					msg3.ID: msg3,
				}

				return original, []uuid.UUID{msg1.ID, msg2.ID, msg3.ID}
			},
			deleteIDs: nil, // будет заполнено из prepareMessages
			validate: func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message) {
				t.Helper()

				for id := range originalMessages {
					_, err := svc.getMessageFromMap(id)
					require.Error(t, err)
					require.Contains(t, err.Error(), "message not found by id")
				}
			},
		},
		{
			name: "positive case: delete subset of messages",
			prepareMessages: func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID) {
				t.Helper()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				msg2 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field2": "value2"},
					Status:        message.StatusInProgress,
					DriverType:    "redis",
					DriverName:    "test-storage-2",
					InstanceID:    1,
					OperationHash: []byte{4, 5, 6},
				}
				msg3 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field3": "value3"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage-3",
					InstanceID:    1,
					OperationHash: []byte{7, 8, 9},
				}

				svc.addMessageToMap(msg1.ID, msg1)
				svc.addMessageToMap(msg2.ID, msg2)
				svc.addMessageToMap(msg3.ID, msg3)

				original := map[uuid.UUID]*message.Message{
					msg1.ID: msg1,
					msg2.ID: msg2,
					msg3.ID: msg3,
				}

				// Удаляем только msg1 и msg2, оставляем msg3
				return original, []uuid.UUID{msg1.ID, msg2.ID}
			},
			deleteIDs: nil, // будет заполнено из prepareMessages
			validate: func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message) {
				t.Helper()

				// Находим ID удаленных сообщений
				deletedCount := 0
				remainingCount := 0

				for id := range originalMessages {
					_, err := svc.getMessageFromMap(id)
					if err != nil {
						deletedCount++
					} else {
						remainingCount++
					}
				}

				// Должно быть удалено 2 сообщения, остаться 1
				assert.Equal(t, 2, deletedCount)
				assert.Equal(t, 1, remainingCount)
			},
		},
		{
			name: "positive case: delete non-existent message",
			prepareMessages: func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID) {
				t.Helper()

				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(msg.ID, msg)

				original := map[uuid.UUID]*message.Message{
					msg.ID: msg,
				}

				// Пытаемся удалить несуществующий ID
				nonExistentID := uuid.New()

				return original, []uuid.UUID{nonExistentID}
			},
			deleteIDs: nil, // будет заполнено из prepareMessages
			validate: func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message) {
				t.Helper()

				// Проверяем, что исходное сообщение осталось нетронутым
				for id, msg := range originalMessages {
					retrievedMsg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)
					assert.Equal(t, msg.ID, retrievedMsg.ID)
				}
			},
		},
		{
			name: "positive case: delete from empty map",
			prepareMessages: func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID) {
				t.Helper()

				// Карта пустая
				return make(map[uuid.UUID]*message.Message), []uuid.UUID{uuid.New()}
			},
			deleteIDs: nil, // будет заполнено из prepareMessages
			validate: func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message) {
				t.Helper()

				// Не должно быть паники, карта остается пустой
				assert.Len(t, originalMessages, 0)
			},
		},
		{
			name: "positive case: delete empty list",
			prepareMessages: func(t *testing.T, svc *Service) (map[uuid.UUID]*message.Message, []uuid.UUID) {
				t.Helper()

				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}
				svc.addMessageToMap(msg.ID, msg)

				original := map[uuid.UUID]*message.Message{
					msg.ID: msg,
				}

				// Передаем пустой список ID
				return original, []uuid.UUID{}
			},
			deleteIDs: nil, // будет заполнено из prepareMessages
			validate: func(t *testing.T, svc *Service, originalMessages map[uuid.UUID]*message.Message) {
				t.Helper()

				// Проверяем, что сообщение осталось нетронутым
				for id, msg := range originalMessages {
					retrievedMsg, err := svc.getMessageFromMap(id)
					require.NoError(t, err)
					assert.Equal(t, msg.ID, retrievedMsg.ID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			messageRepo := mocks.NewMockmessageRepo(ctrl)

			svc := &Service{
				messageRepo: messageRepo,
				messages:    make(map[uuid.UUID]*message.Message),
			}

			originalMessages, deleteIDs := tt.prepareMessages(t, svc)
			tt.deleteIDs = deleteIDs

			// Выполняем удаление
			svc.deleteMessagesFromMap(tt.deleteIDs)

			// Валидация
			tt.validate(t, svc, originalMessages)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestAddMessageToMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		messages []struct {
			id      uuid.UUID
			message *message.Message
		}
		validate func(t *testing.T, svc *Service, messages []struct {
			id      uuid.UUID
			message *message.Message
		})
	}{
		{
			name: "positive case: add single message",
			messages: []struct {
				id      uuid.UUID
				message *message.Message
			}{
				{
					id: uuid.New(),
					message: &message.Message{
						ID:            uuid.New(),
						Data:          map[string]interface{}{"field1": "value1"},
						Status:        message.StatusInProgress,
						DriverType:    "postgres",
						DriverName:    "test-storage",
						InstanceID:    1,
						OperationHash: []byte{1, 2, 3},
					},
				},
			},
			validate: func(t *testing.T, svc *Service, messages []struct {
				id      uuid.UUID
				message *message.Message
			}) {
				t.Helper()

				for _, msgData := range messages {
					retrievedMsg, err := svc.getMessageFromMap(msgData.id)
					require.NoError(t, err)

					assert.Equal(t, msgData.message, retrievedMsg)
				}
			},
		},
		{
			name: "positive case: add multiple messages",
			messages: []struct {
				id      uuid.UUID
				message *message.Message
			}{
				{
					id: uuid.New(),
					message: &message.Message{
						ID:            uuid.New(),
						Data:          map[string]interface{}{"field1": "value1"},
						Status:        message.StatusInProgress,
						DriverType:    "postgres",
						DriverName:    "test-storage",
						InstanceID:    1,
						OperationHash: []byte{1, 2, 3},
					},
				},
				{
					id: uuid.New(),
					message: &message.Message{
						ID:            uuid.New(),
						Data:          map[string]interface{}{"field2": "value2"},
						Status:        message.StatusValidated,
						DriverType:    "redis",
						DriverName:    "test-storage-2",
						InstanceID:    2,
						OperationHash: []byte{4, 5, 6},
					},
				},
				{
					id: uuid.New(),
					message: &message.Message{
						ID:            uuid.New(),
						Data:          map[string]interface{}{"field3": "value3"},
						Status:        message.StatusFailed,
						DriverType:    "postgres",
						DriverName:    "test-storage-3",
						InstanceID:    3,
						OperationHash: []byte{7, 8, 9},
						Error:         "test error",
					},
				},
			},
			validate: func(t *testing.T, svc *Service, messages []struct {
				id      uuid.UUID
				message *message.Message
			}) {
				t.Helper()

				require.Len(t, messages, 3)

				retrievedMsg1, err := svc.getMessageFromMap(messages[0].id)
				require.NoError(t, err)
				assert.Equal(t, messages[0].message, retrievedMsg1)

				retrievedMsg2, err := svc.getMessageFromMap(messages[1].id)
				require.NoError(t, err)
				assert.Equal(t, messages[1].message, retrievedMsg2)

				retrievedMsg3, err := svc.getMessageFromMap(messages[2].id)
				require.NoError(t, err)
				assert.Equal(t, messages[2].message, retrievedMsg3)
			},
		},
		{
			name: "positive case: overwrite existing message",
			messages: []struct {
				id      uuid.UUID
				message *message.Message
			}{
				{
					id: uuid.New(),
					message: &message.Message{
						ID:            uuid.New(),
						Data:          map[string]interface{}{"field1": "value1"},
						Status:        message.StatusInProgress,
						DriverType:    "postgres",
						DriverName:    "test-storage",
						InstanceID:    1,
						OperationHash: []byte{1, 2, 3},
					},
				},
			},
			validate: func(t *testing.T, svc *Service, messages []struct {
				id      uuid.UUID
				message *message.Message
			}) {
				t.Helper()

				id := uuid.New()

				originalMsg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "original_value"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				// Добавляем первое сообщение
				svc.addMessageToMap(id, originalMsg)

				// Проверяем, что оно добавилось
				retrievedMsg, err := svc.getMessageFromMap(id)
				require.NoError(t, err)
				assert.Equal(t, "original_value", retrievedMsg.Data["field1"])

				// Добавляем новое сообщение с тем же ID (перезаписываем)
				newMsg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "new_value"},
					Status:        message.StatusValidated,
					DriverType:    "redis",
					DriverName:    "test-storage-2",
					InstanceID:    2,
					OperationHash: []byte{4, 5, 6},
				}

				svc.addMessageToMap(id, newMsg)

				// Проверяем, что сообщение было перезаписано
				retrievedMsg2, err := svc.getMessageFromMap(id)
				require.NoError(t, err)
				assert.Equal(t, newMsg.ID, retrievedMsg2.ID)
				assert.Equal(t, "new_value", retrievedMsg2.Data["field1"])
				assert.Equal(t, message.StatusValidated, retrievedMsg2.Status)
				assert.Equal(t, "redis", retrievedMsg2.DriverType)
			},
		},
		{
			name: "positive case: add to empty map",
			messages: []struct {
				id      uuid.UUID
				message *message.Message
			}{},
			validate: func(t *testing.T, svc *Service, messages []struct {
				id      uuid.UUID
				message *message.Message
			}) {
				t.Helper()

				_ = messages // не используется в этом тесте

				// Карта пустая, добавляем сообщение
				id := uuid.New()
				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id, msg)

				// Проверяем, что сообщение добавилось
				retrievedMsg, err := svc.getMessageFromMap(id)
				require.NoError(t, err)
				assert.Equal(t, msg.ID, retrievedMsg.ID)
				assert.Equal(t, msg.Data, retrievedMsg.Data)
			},
		},
		{
			name: "positive case: add message with different ID",
			messages: []struct {
				id      uuid.UUID
				message *message.Message
			}{
				{
					id: uuid.New(),
					message: &message.Message{
						ID:            uuid.New(),
						Data:          map[string]interface{}{"field1": "value1"},
						Status:        message.StatusInProgress,
						DriverType:    "postgres",
						DriverName:    "test-storage",
						InstanceID:    1,
						OperationHash: []byte{1, 2, 3},
					},
				},
			},
			validate: func(t *testing.T, svc *Service, messages []struct {
				id      uuid.UUID
				message *message.Message
			}) {
				t.Helper()

				_ = messages // не используется в этом тесте

				// Добавляем два сообщения с разными ID, но одинаковыми данными
				id1 := uuid.New()
				id2 := uuid.New()

				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				msg2 := &message.Message{
					ID:            msg1.ID, // То же самое внутреннее ID сообщения
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id1, msg1)
				svc.addMessageToMap(id2, msg2)

				// Проверяем, что оба сообщения доступны по своим ключам
				retrievedMsg1, err := svc.getMessageFromMap(id1)
				require.NoError(t, err)
				assert.Equal(t, msg1.ID, retrievedMsg1.ID)

				retrievedMsg2, err := svc.getMessageFromMap(id2)
				require.NoError(t, err)
				assert.Equal(t, msg2.ID, retrievedMsg2.ID)

				// Проверяем, что это разные записи в карте (по ключу, но могут ссылаться на один объект)
				assert.Equal(t, retrievedMsg1.ID, retrievedMsg2.ID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			messageRepo := mocks.NewMockmessageRepo(ctrl)

			svc := &Service{
				messageRepo: messageRepo,
				messages:    make(map[uuid.UUID]*message.Message),
			}

			// Добавляем сообщения из структуры теста
			for _, msgData := range tt.messages {
				svc.addMessageToMap(msgData.id, msgData.message)
			}

			// Валидация
			tt.validate(t, svc, tt.messages)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, похожие тест-кейсы
func TestGetMessageFromMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prepare  func(t *testing.T, svc *Service) (uuid.UUID, *message.Message)
		validate func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message)
	}{
		{
			name: "positive case: get existing message",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				id := uuid.New()
				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id, msg)

				return id, msg
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg, err := svc.getMessageFromMap(id)
				require.NoError(t, err)
				assert.Equal(t, expectedMsg.ID, retrievedMsg.ID)
				assert.Equal(t, expectedMsg.Data, retrievedMsg.Data)
				assert.Equal(t, expectedMsg.Status, retrievedMsg.Status)
				assert.Equal(t, expectedMsg.DriverType, retrievedMsg.DriverType)
				assert.Equal(t, expectedMsg.DriverName, retrievedMsg.DriverName)
				assert.Equal(t, expectedMsg.InstanceID, retrievedMsg.InstanceID)
				assert.Equal(t, expectedMsg.OperationHash, retrievedMsg.OperationHash)
			},
		},
		{
			name: "positive case: get message with error field",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				id := uuid.New()
				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusFailed,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
					Error:         "test error message",
				}

				svc.addMessageToMap(id, msg)

				return id, msg
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg, err := svc.getMessageFromMap(id)
				require.NoError(t, err)
				assert.Equal(t, expectedMsg.ID, retrievedMsg.ID)
				assert.Equal(t, expectedMsg.Error, retrievedMsg.Error)
				assert.Equal(t, expectedMsg.Status, retrievedMsg.Status)
			},
		},
		{
			name: "negative case: get non-existent message",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				// Добавляем одно сообщение
				id1 := uuid.New()
				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id1, msg)

				// Возвращаем ID, которого нет в карте
				nonExistentID := uuid.New()

				return nonExistentID, nil
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg, err := svc.getMessageFromMap(id)
				require.Error(t, err)
				require.Nil(t, retrievedMsg)
				require.Contains(t, err.Error(), "message not found by id")
				require.Contains(t, err.Error(), id.String())
			},
		},
		{
			name: "negative case: get from empty map",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				// Карта пустая, возвращаем любой ID
				return uuid.New(), nil
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg, err := svc.getMessageFromMap(id)
				require.Error(t, err)
				require.Nil(t, retrievedMsg)
				require.Contains(t, err.Error(), "message not found by id")
				require.Contains(t, err.Error(), id.String())
			},
		},
		{
			name: "negative case: get after deletion",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				id := uuid.New()
				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id, msg)

				// Удаляем сообщение
				svc.deleteMessagesFromMap([]uuid.UUID{id})

				return id, nil
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg, err := svc.getMessageFromMap(id)
				require.Error(t, err)
				require.Nil(t, retrievedMsg)
				require.Contains(t, err.Error(), "message not found by id")
				require.Contains(t, err.Error(), id.String())
			},
		},
		{
			name: "positive case: get after overwrite",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				id := uuid.New()

				originalMsg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "original_value"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id, originalMsg)

				// Перезаписываем сообщение
				newMsg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "new_value"},
					Status:        message.StatusValidated,
					DriverType:    "redis",
					DriverName:    "test-storage-2",
					InstanceID:    2,
					OperationHash: []byte{4, 5, 6},
				}

				svc.addMessageToMap(id, newMsg)

				return id, newMsg
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg, err := svc.getMessageFromMap(id)
				require.NoError(t, err)
				assert.Equal(t, expectedMsg.ID, retrievedMsg.ID)
				assert.Equal(t, expectedMsg.Data, retrievedMsg.Data)
				assert.Equal(t, expectedMsg.Status, retrievedMsg.Status)
				assert.Equal(t, expectedMsg.DriverType, retrievedMsg.DriverType)
				assert.Equal(t, expectedMsg.DriverName, retrievedMsg.DriverName)
			},
		},
		{
			name: "positive case: get multiple different messages",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				// Добавляем несколько сообщений
				id1 := uuid.New()
				msg1 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				id2 := uuid.New()
				msg2 := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field2": "value2"},
					Status:        message.StatusValidated,
					DriverType:    "redis",
					DriverName:    "test-storage-2",
					InstanceID:    2,
					OperationHash: []byte{4, 5, 6},
				}

				svc.addMessageToMap(id1, msg1)
				svc.addMessageToMap(id2, msg2)

				// Сохраняем оба ID в сообщении для использования в validate
				// Используем поле Data для хранения второго ID (временное решение)
				msg1.Data["_test_id2"] = id2.String()
				msg1.Data["_test_msg2_data"] = msg2.Data

				return id1, msg1
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				// Проверяем первое сообщение
				retrievedMsg1, err := svc.getMessageFromMap(id)
				require.NoError(t, err)

				assert.Equal(t, expectedMsg.ID, retrievedMsg1.ID)
				assert.Equal(t, expectedMsg.Data["field1"], retrievedMsg1.Data["field1"])
				assert.Equal(t, expectedMsg.Status, retrievedMsg1.Status)

				// Получаем второй ID из временного поля
				id2Str, ok := expectedMsg.Data["_test_id2"].(string)
				require.True(t, ok)

				id2, err := uuid.Parse(id2Str)
				require.NoError(t, err)

				// Проверяем второе сообщение
				retrievedMsg2, err := svc.getMessageFromMap(id2)
				require.NoError(t, err)

				assert.Equal(t, message.StatusValidated, retrievedMsg2.Status)
				assert.Equal(t, "redis", retrievedMsg2.DriverType)
				assert.Equal(t, map[string]interface{}{"field2": "value2"}, retrievedMsg2.Data)
			},
		},
		{
			name: "positive case: get returns same pointer",
			prepare: func(t *testing.T, svc *Service) (uuid.UUID, *message.Message) {
				t.Helper()

				id := uuid.New()
				msg := &message.Message{
					ID:            uuid.New(),
					Data:          map[string]interface{}{"field1": "value1"},
					Status:        message.StatusInProgress,
					DriverType:    "postgres",
					DriverName:    "test-storage",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3},
				}

				svc.addMessageToMap(id, msg)

				return id, msg
			},
			validate: func(t *testing.T, svc *Service, id uuid.UUID, expectedMsg *message.Message) {
				t.Helper()

				retrievedMsg1, err := svc.getMessageFromMap(id)
				require.NoError(t, err)

				retrievedMsg2, err := svc.getMessageFromMap(id)
				require.NoError(t, err)

				// Проверяем, что возвращается один и тот же указатель
				assert.Same(t, expectedMsg, retrievedMsg1)
				assert.Same(t, expectedMsg, retrievedMsg2)
				assert.Same(t, retrievedMsg1, retrievedMsg2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			messageRepo := mocks.NewMockmessageRepo(ctrl)

			svc := &Service{
				messageRepo: messageRepo,
				messages:    make(map[uuid.UUID]*message.Message),
			}

			id, expectedMsg := tt.prepare(t, svc)

			// Валидация
			tt.validate(t, svc, id, expectedMsg)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestUpdateMetricsFromStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		originalStatus message.Status
		newStatus      message.Status
		count          int
		setupMocks     func(t *testing.T, metricsService *mocks.MockmessageCounter)
	}{
		{
			name:           "update metrics from in progress to failed",
			originalStatus: message.StatusInProgress,
			newStatus:      message.StatusFailed,
			count:          1,
			setupMocks: func(t *testing.T, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				metricsService.EXPECT().AddFailedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1)
			},
		},
		{
			name:           "update metrics from in progress to validated",
			originalStatus: message.StatusInProgress,
			newStatus:      message.StatusValidated,
			count:          1,
			setupMocks: func(t *testing.T, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementProcessingMessagesBy(gomock.Any()).Return().Times(1)
			},
		},
		{
			name:           "update metrics from failed to validated",
			originalStatus: message.StatusFailed,
			newStatus:      message.StatusValidated,
			count:          1,
			setupMocks: func(t *testing.T, metricsService *mocks.MockmessageCounter) {
				t.Helper()

				metricsService.EXPECT().AddValidatedMessages(gomock.Any()).Return().Times(1)
				metricsService.EXPECT().DecrementFailedMessagesBy(gomock.Any()).Return().Times(1)
			},
		},
		{
			name:           "equal statuses",
			originalStatus: message.StatusInProgress,
			newStatus:      message.StatusInProgress,
			count:          1,
			setupMocks: func(t *testing.T, metricsService *mocks.MockmessageCounter) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			metricsService := mocks.NewMockmessageCounter(ctrl)

			srv := &Service{
				metricsService: metricsService,
			}

			tt.setupMocks(t, metricsService)

			srv.updateMetricsFromStatuses(tt.originalStatus, tt.newStatus, tt.count)
		})
	}
}
