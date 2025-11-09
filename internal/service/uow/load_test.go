package uow

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/uow/mocks"
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
		createSvc  func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service
		setupMocks func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").Times(1)

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).Times(1)

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
			wantErr: require.NoError,
		},
		{
			name: "positive case: operation hash is different",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
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
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: get all transactions by fields error",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

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
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
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

				requestsRepo.EXPECT().GetAllTransactionsByFields(gomock.Any(), gomock.Any()).Return([]storage.TransactionModel{txModel}, nil).Times(1).Do(func(ctx context.Context, fields map[string]any) {
					expectedFields := map[string]any{
						"status":         string(storage.TxStatusInProgress),
						"instance_id":    1,
						"operation_type": operation.OperationTypeCreate,
					}

					assert.Equal(t, expectedFields, fields)
				})

				requestsRepo.EXPECT().UpdateStatusMany(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1).Do(func(ctx context.Context, ids []string, status string, errMsg string) {
					expectedIDs := []string{txID}
					expectedStatus := string(storage.TxStatusCanceled)

					assert.Equal(t, expectedIDs, ids)
					assert.Equal(t, expectedStatus, status)
				})
			},
			wantErr: require.Error,
		},
		{
			name: "error case: build requests error",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)

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
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				svc := &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)

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

			requestsRepo := mocks.NewMockrequestsRepo(ctrl)
			userDriver := storagemocks.NewMockDriver(ctrl)

			tt.setupMocks(t, requestsRepo, userDriver)

			err := tt.createSvc(t, requestsRepo, userDriver).LoadOnStartup(t.Context())
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestProcessTxModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service
		setupMocks func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: build requests error",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
			},
			wantErr: require.Error,
		},
		{
			name: "error case: exec tx error",
			createSvc: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					requestsRepo: requestsRepo,
					instanceID:   1,
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
			setupMocks: func(t *testing.T, requestsRepo *mocks.MockrequestsRepo, userDriver *storagemocks.MockDriver) {
				t.Helper()

				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestsRepo := mocks.NewMockrequestsRepo(ctrl)
			userDriver := storagemocks.NewMockDriver(ctrl)

			tt.setupMocks(t, requestsRepo, userDriver)

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

			err := tt.createSvc(t, requestsRepo, userDriver).processTxModel(t.Context(), txModel)
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
