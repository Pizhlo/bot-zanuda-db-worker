package uow

import (
	"context"
	"db-worker/internal/config/operation"
	uowmocks "db-worker/internal/service/uow/mocks"
	"db-worker/internal/storage"
	storagemocks "db-worker/internal/storage/mocks"
	"db-worker/pkg/random"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestLoadOnStartup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service
		setupMocks func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}

				return svc
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").Times(1)

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).Times(1)

				// AddTotalTransactions вызывается: 1 раз с 1 (из processTxModel) и 3 раза с 0 (из setupMetrics)
				metricsService.EXPECT().AddTotalTransactions(1).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(3)
				metricsService.EXPECT().AddInProgressTransactions(1).Times(1)
				// AddSuccessTransactions вызывается: 1 раз с 1 (из execTx defer через addSuccessTransactions)
				metricsService.EXPECT().AddSuccessTransactions(1).Times(1)
				// DecrementInProgressTransactions вызывается один раз: из addSuccessTransactions(1) в execTx defer
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)

				txID := random.String(10)

				txModel := storage.TransactionModel{
					ID:            txID,
					Status:        storage.TxStatusInProgress,
					InstanceID:    1,
					OperationType: string(operation.OperationTypeCreate),
					OperationHash: []byte{0x1, 0x2, 0x3},
					Data: map[string]any{
						"id": 1,
					},
					Requests: []storage.RequestModel{
						{
							ID:         uuid.New(),
							TxID:       txID,
							DriverType: string(operation.StorageTypePostgres),
							DriverName: "test-storage",
						},
					},
					CreatedAt: time.Now(),
				}

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return([]storage.TransactionModel{txModel}, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})

				// setupMetrics calls
				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, nil).Times(3).Do(func(ctx context.Context, fields map[string]any) {
					status, ok := fields["status"].(string)
					require.True(t, ok)
					assert.Contains(t, []string{string(storage.TxStatusSuccess), string(storage.TxStatusFailed), string(storage.TxStatusCanceled)}, status)
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(0).Times(1)
				metricsService.EXPECT().AddFailedTransactions(0).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(0).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: operation hash is different",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}

				return svc
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				txID := random.String(10)

				txModel := storage.TransactionModel{
					ID:            txID,
					Status:        storage.TxStatusInProgress,
					InstanceID:    1,
					OperationType: string(operation.OperationTypeCreate),
					OperationHash: []byte{0x4, 0x5, 0x6},
					Data: map[string]any{
						"id": 1,
					},
					Requests: []storage.RequestModel{
						{
							ID:         uuid.New(),
							TxID:       txID,
							DriverType: string(operation.StorageTypePostgres),
							DriverName: "test-storage",
						},
					},
					CreatedAt: time.Now(),
				}

				// AddTotalTransactions вызывается: 1 раз с 1 (из цикла LoadOnStartup для отмененной транзакции) и 3 раза с 0 (из setupMetrics)
				metricsService.EXPECT().AddTotalTransactions(1).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(3)
				metricsService.EXPECT().AddInProgressTransactions(1).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(1).Times(1)
				// DecrementInProgressTransactions вызывается из addCanceledTransactions
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return([]storage.TransactionModel{txModel}, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})

				requestsRepo.EXPECT().UpdateStatusMany(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1).Do(func(ctx context.Context, ids []string, status string, errMsg string) {
					expectedIDs := []string{txID}
					expectedStatus := string(storage.TxStatusCanceled)

					assert.Equal(t, expectedIDs, ids)
					assert.Equal(t, expectedStatus, status)
				})

				// setupMetrics calls
				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, nil).Times(3).Do(func(ctx context.Context, fields map[string]any) {
					status, ok := fields["status"].(string)
					require.True(t, ok)
					assert.Contains(t, []string{string(storage.TxStatusSuccess), string(storage.TxStatusFailed), string(storage.TxStatusCanceled)}, status)
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				// setupMetrics вызывает AddSuccessTransactions, AddFailedTransactions и AddCanceledTransactions с 0
				metricsService.EXPECT().AddSuccessTransactions(0).Times(1)
				metricsService.EXPECT().AddFailedTransactions(0).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(0).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: get all transactions by fields error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}

				return svc
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				// setupMetrics calls
				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, nil).Times(3).Do(func(ctx context.Context, fields map[string]any) {
					status, ok := fields["status"].(string)
					require.True(t, ok)
					assert.Contains(t, []string{string(storage.TxStatusSuccess), string(storage.TxStatusFailed), string(storage.TxStatusCanceled)}, status)
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(0).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(3)
				metricsService.EXPECT().AddFailedTransactions(0).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(0).Times(1)

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return(nil, errors.New("test error")).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})
			},
			wantErr: require.Error,
		},
		{
			name: "error case: update status many error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}

				return svc
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				// setupMetrics calls
				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, nil).Times(3).Do(func(ctx context.Context, fields map[string]any) {
					status, ok := fields["status"].(string)
					require.True(t, ok)
					assert.Contains(t, []string{string(storage.TxStatusSuccess), string(storage.TxStatusFailed), string(storage.TxStatusCanceled)}, status)
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(0).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(1)
				metricsService.EXPECT().AddFailedTransactions(0).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(0).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(1)

				txID := random.String(10)

				txModel := storage.TransactionModel{
					ID:            txID,
					Status:        storage.TxStatusInProgress,
					InstanceID:    1,
					OperationType: string(operation.OperationTypeCreate),
					OperationHash: []byte{0x4, 0x5, 0x6},
					Data: map[string]any{
						"id": 1,
					},
					Requests: []storage.RequestModel{
						{
							ID:         uuid.New(),
							TxID:       txID,
							DriverType: string(operation.StorageTypePostgres),
							DriverName: "test-storage",
						},
					},
					CreatedAt: time.Now(),
				}

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return([]storage.TransactionModel{txModel}, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})

				metricsService.EXPECT().AddTotalTransactions(1).Times(1)
				metricsService.EXPECT().AddInProgressTransactions(1).Times(1)

				requestsRepo.EXPECT().UpdateStatusMany(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1).Do(func(ctx context.Context, ids []string, status string, errMsg string) {
					expectedIDs := []string{txID}
					expectedStatus := string(storage.TxStatusCanceled)

					assert.Equal(t, expectedIDs, ids)
					assert.Equal(t, expectedStatus, status)
				})

				// defer в LoadOnStartup вызывает addFailedTransactions(1) при ошибке
				metricsService.EXPECT().AddFailedTransactions(1).Times(1)
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)
			},
			wantErr: require.Error,
		},
		{
			name: "error case: build requests error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}

				return svc
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				// setupMetrics calls
				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, nil).Times(3).Do(func(ctx context.Context, fields map[string]any) {
					status, ok := fields["status"].(string)
					require.True(t, ok)
					assert.Contains(t, []string{string(storage.TxStatusSuccess), string(storage.TxStatusFailed), string(storage.TxStatusCanceled)}, status)
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(0).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(3)
				metricsService.EXPECT().AddFailedTransactions(0).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(0).Times(1)

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)

				metricsService.EXPECT().AddTotalTransactions(1).Times(1)
				metricsService.EXPECT().AddInProgressTransactions(1).Times(1)
				// При ошибке в BuildRequests вызывается addFailedTransactions один раз:
				// из defer в LoadOnStartup при ошибке (1)
				metricsService.EXPECT().AddFailedTransactions(1).Times(1)
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)

				txID := random.String(10)

				txModel := storage.TransactionModel{
					ID:            txID,
					Status:        storage.TxStatusInProgress,
					InstanceID:    1,
					OperationType: string(operation.OperationTypeCreate),
					OperationHash: []byte{0x1, 0x2, 0x3},
					Data: map[string]any{
						"id": 1,
					},
					Requests: []storage.RequestModel{
						{
							ID:         uuid.New(),
							TxID:       txID,
							DriverType: string(operation.StorageTypePostgres),
							DriverName: "test-storage",
						},
					},
					CreatedAt: time.Now(),
				}

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return([]storage.TransactionModel{txModel}, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})
			},
			wantErr: require.Error,
		},
		{
			name: "error case: exec tx error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}

				return svc
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				// setupMetrics calls
				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, nil).Times(3).Do(func(ctx context.Context, fields map[string]any) {
					status, ok := fields["status"].(string)
					require.True(t, ok)
					assert.Contains(t, []string{string(storage.TxStatusSuccess), string(storage.TxStatusFailed), string(storage.TxStatusCanceled)}, status)
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(0).Times(1)
				metricsService.EXPECT().AddTotalTransactions(0).Times(3)
				metricsService.EXPECT().AddFailedTransactions(0).Times(1)
				metricsService.EXPECT().AddCanceledTransactions(0).Times(1)

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)

				metricsService.EXPECT().AddTotalTransactions(1).Times(1)
				metricsService.EXPECT().AddInProgressTransactions(1).Times(1)
				// При ошибке в execTx вызывается addFailedTransactions один раз:
				// из defer в LoadOnStartup при ошибке (1)
				metricsService.EXPECT().AddFailedTransactions(1).Times(1)
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)

				txID := random.String(10)

				txModel := storage.TransactionModel{
					ID:            txID,
					Status:        storage.TxStatusInProgress,
					InstanceID:    1,
					OperationType: string(operation.OperationTypeCreate),
					OperationHash: []byte{0x1, 0x2, 0x3},
					Data: map[string]any{
						"id": 1,
					},
					Requests: []storage.RequestModel{
						{
							ID:         uuid.New(),
							TxID:       txID,
							DriverType: string(operation.StorageTypePostgres),
							DriverName: "test-storage",
						},
					},
					CreatedAt: time.Now(),
				}

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return([]storage.TransactionModel{txModel}, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestsRepo := uowmocks.NewMockrequestsRepo(ctrl)
			userDriver := storagemocks.NewMockDriver(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			tt.setupMocks(t, requestsRepo, userDriver, metricsService)

			err := tt.createSvc(t, requestsRepo, userDriver, metricsService).LoadOnStartup(t.Context())
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestProcessTxModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service
		setupMocks func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).Times(1)

				// processTxModel в defer вызывает addSuccessTransactions при успехе
				metricsService.EXPECT().AddSuccessTransactions(1).Times(1)
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: build requests error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				// processTxModel в defer вызывает addFailedTransactions при ошибке
				metricsService.EXPECT().AddFailedTransactions(1).Times(1)
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)
			},
			wantErr: require.Error,
		},
		{
			name: "error case: exec tx error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
						Type: operation.OperationTypeCreate,
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					}, userStoragesMap: map[string]storage.Driver{
						"test-storage": userDriver,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, userDriver *storagemocks.MockDriver, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)

				// processTxModel в defer вызывает addFailedTransactions при ошибке
				metricsService.EXPECT().AddFailedTransactions(1).Times(1)
				metricsService.EXPECT().DecrementInProgressTransactions(1).Times(1)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestsRepo := uowmocks.NewMockrequestsRepo(ctrl)
			userDriver := storagemocks.NewMockDriver(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			tt.setupMocks(t, requestsRepo, userDriver, metricsService)

			txID := random.String(10)

			txModel := storage.TransactionModel{
				ID:            txID,
				Status:        storage.TxStatusInProgress,
				InstanceID:    1,
				OperationType: string(operation.OperationTypeCreate),
				OperationHash: []byte{0x1, 0x2, 0x3},
				Data: map[string]any{
					"id": 1,
				},
				Requests: []storage.RequestModel{
					{
						ID:         uuid.New(),
						TxID:       txID,
						DriverType: string(operation.StorageTypePostgres),
						DriverName: "test-storage",
					},
				},
				CreatedAt: time.Now(),
			}

			err := tt.createSvc(t, requestsRepo, userDriver, metricsService).processTxModel(t.Context(), txModel)
			tt.wantErr(t, err)
		})
	}
}

func TestCompareOperationHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		operationHash []byte
		cfgHash       []byte
		want          bool
	}{
		{
			name:          "positive case",
			operationHash: []byte{0x1, 0x2, 0x3},
			cfgHash:       []byte{0x1, 0x2, 0x3},
			want:          true,
		},
		{
			name:          "negative case",
			operationHash: []byte{0x1, 0x2, 0x3},
			cfgHash:       []byte{0x4, 0x5, 0x6},
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := compareOperationHash(tt.operationHash, tt.cfgHash)
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, схожие тест-кейсы
func TestSetupSuccessTransactionMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service
		setupMocks func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeCreate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				expectedCount := 5

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(expectedCount, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusSuccess), fields["status"])
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(expectedCount).Times(1)
				metricsService.EXPECT().AddTotalTransactions(expectedCount).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: GetCountTransactionsByFields error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeCreate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, errors.New("database error")).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusSuccess), fields["status"])
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: zero count",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     2,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeUpdate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				expectedCount := 0

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(expectedCount, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusSuccess), fields["status"])
					assert.Equal(t, 2, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeUpdate, fields["operation_type"])
				})

				metricsService.EXPECT().AddSuccessTransactions(expectedCount).Times(1)
				metricsService.EXPECT().AddTotalTransactions(expectedCount).Times(1)
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestsRepo := uowmocks.NewMockrequestsRepo(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			tt.setupMocks(t, requestsRepo, metricsService)

			err := tt.createSvc(t, requestsRepo, metricsService).setupSuccessTransactionMetrics(context.Background())
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, схожие тест-кейсы
func TestSetupFailedTransactionMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service
		setupMocks func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeCreate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				expectedCount := 10

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(expectedCount, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusFailed), fields["status"])
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddFailedTransactions(expectedCount).Times(1)
				metricsService.EXPECT().AddTotalTransactions(expectedCount).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: GetCountTransactionsByFields error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeCreate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, errors.New("database error")).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusFailed), fields["status"])
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: zero count",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     3,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeDelete,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				expectedCount := 0

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(expectedCount, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusFailed), fields["status"])
					assert.Equal(t, 3, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeDelete, fields["operation_type"])
				})

				metricsService.EXPECT().AddFailedTransactions(expectedCount).Times(1)
				metricsService.EXPECT().AddTotalTransactions(expectedCount).Times(1)
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestsRepo := uowmocks.NewMockrequestsRepo(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			tt.setupMocks(t, requestsRepo, metricsService)

			err := tt.createSvc(t, requestsRepo, metricsService).setupFailedTransactionMetrics(context.Background())
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, схожие тест-кейсы
func TestSetupCanceledTransactionMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service
		setupMocks func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeCreate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				expectedCount := 7

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(expectedCount, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusCanceled), fields["status"])
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})

				metricsService.EXPECT().AddCanceledTransactions(expectedCount).Times(1)
				metricsService.EXPECT().AddTotalTransactions(expectedCount).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: GetCountTransactionsByFields error",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     1,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeCreate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(0, errors.New("database error")).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusCanceled), fields["status"])
					assert.Equal(t, 1, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeCreate, fields["operation_type"])
				})
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: zero count",
			createSvc: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) *Service {
				t.Helper()

				return &Service{
					requestsRepo:   requestsRepo,
					metricsService: metricsService,
					instanceID:     4,
					cfg: &operation.Operation{
						Name: "test-operation",
						Type: operation.OperationTypeUpdate,
					},
				}
			},
			setupMocks: func(t *testing.T, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) {
				t.Helper()

				expectedCount := 0

				requestsRepo.EXPECT().GetCountTransactionsByFields(gomock.Any(), gomock.Any()).Return(expectedCount, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					assert.Equal(t, string(storage.TxStatusCanceled), fields["status"])
					assert.Equal(t, 4, fields["instance_id"])
					assert.Equal(t, operation.OperationTypeUpdate, fields["operation_type"])
				})

				metricsService.EXPECT().AddCanceledTransactions(expectedCount).Times(1)
				metricsService.EXPECT().AddTotalTransactions(expectedCount).Times(1)
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestsRepo := uowmocks.NewMockrequestsRepo(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			tt.setupMocks(t, requestsRepo, metricsService)

			err := tt.createSvc(t, requestsRepo, metricsService).setupCanceledTransactionMetrics(context.Background())
			tt.wantErr(t, err)
		})
	}
}
