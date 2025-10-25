package uow

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"db-worker/internal/storage/mocks"
	"db-worker/internal/storage/testtransaction"
	"db-worker/pkg/random"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestBeginTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createSvc        func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		createRequests   func(storageDriver storage.Driver) map[storage.Driver]*storage.Request
		createExpectedTx func(storageDriver storage.Driver) storage.TransactionEditor
		setupMocks       func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver)
		checkTx          func(t *testing.T, expectedTx storage.TransactionEditor, actualTx storage.TransactionEditor)
		wantErr          require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
					},
					storage: systemDriver,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
					systemStoragesMap: map[string]storage.Driver{
						StorageNameForTransactionsTable: systemDriver,
						StorageNameForRequestsTable:     systemDriver,
					},
					systemStorageConfigs: []operation.StorageCfg{
						{
							Name:  StorageNameForTransactionsTable,
							Type:  operation.StorageTypePostgres,
							Table: "transactions.transactions",
						},
						{
							Name:  StorageNameForRequestsTable,
							Type:  operation.StorageTypePostgres,
							Table: "transactions.requests",
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createExpectedTx: func(storageDriver storage.Driver) storage.TransactionEditor {
				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithBegun(map[storage.Driver]struct{}{
						storageDriver: {},
					}),
					testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
				)
			},
			checkTx: func(t *testing.T, expectedTx storage.TransactionEditor, actualTx storage.TransactionEditor) {
				t.Helper()

				assert.Equal(t, expectedTx.Begun(), actualTx.Begun())
				assert.Equal(t, expectedTx.Requests(), actualTx.Requests())
				assert.Equal(t, expectedTx.Error(), actualTx.Error())
				assert.Equal(t, expectedTx.FailedDriver(), actualTx.FailedDriver())
				assert.Equal(t, expectedTx.Status(), actualTx.Status())
				assert.Equal(t, expectedTx.InstanceID(), actualTx.InstanceID())
				assert.Equal(t, expectedTx.OperationHash(), actualTx.OperationHash())
				assert.Equal(t, expectedTx.Drivers(), actualTx.Drivers())

				assert.Len(t, actualTx.ID(), 20)
			},
			setupMocks: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: failed to begin transaction in driver",
			createSvc: func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
					},
					storage: systemDriver,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
					systemStoragesMap: map[string]storage.Driver{
						StorageNameForTransactionsTable: systemDriver,
						StorageNameForRequestsTable:     systemDriver,
					},
					systemStorageConfigs: []operation.StorageCfg{
						{
							Name:  StorageNameForTransactionsTable,
							Type:  operation.StorageTypePostgres,
							Table: "transactions.transactions",
						},
						{
							Name:  StorageNameForRequestsTable,
							Type:  operation.StorageTypePostgres,
							Table: "transactions.requests",
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createExpectedTx: func(storageDriver storage.Driver) storage.TransactionEditor {
				return nil // возвращаем nil из-за ошибки
			},
			checkTx: func(t *testing.T, _, actualTx storage.TransactionEditor) {
				t.Helper()

				assert.Nil(t, actualTx)
			},
			setupMocks: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("error"))
			},
			wantErr: require.Error,
		},
		{
			name: "error in newTx",
			createSvc: func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
					},
					storage: systemDriver,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
					systemStoragesMap: map[string]storage.Driver{
						StorageNameForTransactionsTable: systemDriver,
						StorageNameForRequestsTable:     systemDriver,
					},
					systemStorageConfigs: []operation.StorageCfg{
						{
							Name:  StorageNameForTransactionsTable,
							Type:  operation.StorageTypePostgres,
							Table: "transactions.transactions",
						},
						{
							Name:  StorageNameForRequestsTable,
							Type:  operation.StorageTypePostgres,
							Table: "transactions.requests",
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return nil
			},
			createExpectedTx: func(storageDriver storage.Driver) storage.TransactionEditor {
				t.Helper()

				return nil
			},
			setupMocks: func(t *testing.T, systemDriver, userDriver *mocks.MockDriver) {
				t.Helper()
			},
			checkTx: func(t *testing.T, _, actualTx storage.TransactionEditor) {
				t.Helper()

				assert.Nil(t, actualTx)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// системное хранилище - это хранилище, в котором сохраняются транзакции
			// пользовательское хранилище - это хранилище, в котором сохраняются данные пользователей
			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			svc := tt.createSvc(systemDriver, userDriver)

			tt.setupMocks(t, systemDriver, userDriver)

			actualTx, err := svc.beginTx(t.Context(), tt.createRequests(userDriver))
			tt.wantErr(t, err)

			tt.checkTx(t, tt.createExpectedTx(userDriver), actualTx)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestNewTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createSvc        func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		createRequests   func(storageDriver storage.Driver) map[storage.Driver]*storage.Request
		createExpectedTx func(storageDriver storage.Driver) storage.TransactionEditor
		setupMocks       func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver)
		checkTx          func(t *testing.T, expectedTx storage.TransactionEditor, actualTx storage.TransactionEditor)
		wantErr          require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				return newTestService(t, systemDriver, userDriver)
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createExpectedTx: func(storageDriver storage.Driver) storage.TransactionEditor {
				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithBegun(map[storage.Driver]struct{}{}), // пустая карта, так как newTx не начинает транзакцию в драйверах
					testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
					testtransaction.WithInstanceID(1),
				)
			},
			setupMocks: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// В newTx транзакция не начинается в пользовательских драйверах,
				// поэтому ожидать Begin на userDriver не нужно.
			},
			checkTx: func(t *testing.T, expectedTx storage.TransactionEditor, actualTx storage.TransactionEditor) {
				t.Helper()

				assert.Equal(t, expectedTx.Begun(), actualTx.Begun())
				assert.Equal(t, expectedTx.Requests(), actualTx.Requests())
				assert.Equal(t, expectedTx.Error(), actualTx.Error())
				assert.Equal(t, expectedTx.FailedDriver(), actualTx.FailedDriver())
				assert.Equal(t, expectedTx.Status(), actualTx.Status())
				assert.Equal(t, expectedTx.InstanceID(), actualTx.InstanceID())
				assert.Equal(t, expectedTx.OperationHash(), actualTx.OperationHash())
				assert.Equal(t, expectedTx.Drivers(), actualTx.Drivers())

				assert.Len(t, actualTx.ID(), 20)
			},
			wantErr: require.NoError,
		},
		{
			name: "error in NewTransaction",
			createSvc: func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				return newTestService(t, systemDriver, userDriver)
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return nil
			},
			createExpectedTx: func(storageDriver storage.Driver) storage.TransactionEditor {
				t.Helper()

				return nil
			},
			setupMocks: func(t *testing.T, systemDriver, userDriver *mocks.MockDriver) {
				t.Helper()
			},
			checkTx: func(t *testing.T, _, actualTx storage.TransactionEditor) {
				t.Helper()

				assert.Nil(t, actualTx)
			},
			wantErr: require.Error,
		},
		{
			name: "error in saveTx",
			createSvc: func(systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				return newTestService(t, systemDriver, userDriver)
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createExpectedTx: func(storageDriver storage.Driver) storage.TransactionEditor {
				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithBegun(map[storage.Driver]struct{}{}), // пустая карта, так как newTx не начинает транзакцию в драйверах
					testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
					testtransaction.WithInstanceID(1),
				)
			},
			setupMocks: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageType("unknown")).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// В newTx/saveTx транзакция начинается только в системных драйверах,
				// поэтому ожидать Begin на userDriver не нужно.
			},
			checkTx: func(t *testing.T, _ storage.TransactionEditor, actualTx storage.TransactionEditor) {
				t.Helper()

				assert.Nil(t, actualTx)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			svc := tt.createSvc(systemDriver, userDriver)

			tt.setupMocks(t, systemDriver, userDriver)

			actualTx, err := svc.newTx(t.Context(), tt.createRequests(userDriver))
			tt.wantErr(t, err)

			tt.checkTx(t, tt.createExpectedTx(userDriver), actualTx)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestBeginInDriver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		createSvc      func(t *testing.T, systemDriver *mocks.MockDriver) *Service
		createRequests func(storageDriver storage.Driver) map[storage.Driver]*storage.Request
		createTx       func(storageDriver storage.Driver) (*storage.Transaction, error)
		checkTx        func(t *testing.T, tx *storage.Transaction, driver storage.Driver)
		setupMocks     func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver)
		wantErr        require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage:    systemDriver,
					instanceID: 1,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createTx: func(storageDriver storage.Driver) (*storage.Transaction, error) {
				t.Helper()

				return storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, _ *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil)
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
				assert.Equal(t, map[storage.Driver]struct{}{driver: {}}, tx.Begun())
			},
			wantErr: require.NoError,
		},
		{
			name: "begin error",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage:    systemDriver,
					instanceID: 1,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createTx: func(storageDriver storage.Driver) (*storage.Transaction, error) {
				t.Helper()

				return storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("error"))
				driver.EXPECT().Name().Return("test-storage").AnyTimes()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			wantErr: require.Error,
		},
		{
			name: "update error",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage:    systemDriver,
					instanceID: 1,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
			},
			createTx: func(storageDriver storage.Driver) (*storage.Transaction, error) {
				t.Helper()

				return storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("error"))
				driver.EXPECT().Name().Return("test-storage").AnyTimes()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(errors.New("commit error")).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			wantErr: require.Error,
		},
		{
			name: "finishTx error",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage:    systemDriver,
					instanceID: 1,
					transactionDriversMap: map[string]DriversMap{
						StorageNameForTransactionsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForTransactionsTable,
								Table: "transactions.transactions",
							},
						},
						StorageNameForRequestsTable: {
							driver: systemDriver,
							cfg: operation.StorageCfg{
								Name:  StorageNameForRequestsTable,
								Table: "transactions.requests",
							},
						},
					},
				}
			},
			createRequests: func(storageDriver storage.Driver) map[storage.Driver]*storage.Request {
				return map[storage.Driver]*storage.Request{
					storageDriver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			createTx: func(storageDriver storage.Driver) (*storage.Transaction, error) {
				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						storageDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				tx.AddBegunDriver(storageDriver)

				return tx, nil
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("error"))
				driver.EXPECT().Name().Return("test-storage").AnyTimes()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// finishTX
				driver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(errors.New("finishTx error"))
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driver := mocks.NewMockDriver(ctrl)
			systemDriver := mocks.NewMockDriver(ctrl)

			svc := tt.createSvc(t, systemDriver)

			tt.setupMocks(t, driver, systemDriver)

			tx, err := tt.createTx(driver)
			require.NoError(t, err)

			err = svc.beginInDriver(t.Context(), tx, driver)
			tt.wantErr(t, err)

			tt.checkTx(t, tx, driver)
		})
	}
}

//nolint:funlen // длинный тест
func TestFinishTx(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	tests := []struct {
		name       string
		svc        *Service
		createTx   func(driver storage.Driver) storage.TransactionEditor
		setupMocks func(t *testing.T, driver *mocks.MockDriver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: func() *Service {
				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
				}
			}(),
			createTx: func(driver storage.Driver) storage.TransactionEditor {
				return testtransaction.NewTestTransaction(
					testtransaction.WithID(txID),
					testtransaction.WithStatus(string(storage.TxStatusSuccess)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				driver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is in progress",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
			},
			createTx: func(driver storage.Driver) storage.TransactionEditor {
				return testtransaction.NewTestTransaction(
					testtransaction.WithID(txID),
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver) {
				t.Helper()
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status equal to: \"IN_PROGRESS\", but expected: \"SUCCESS\" or \"FAILED")
			}),
		},
		{
			name: "error in driver",
			svc: func() *Service {
				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
				}
			}(),
			createTx: func(driver storage.Driver) storage.TransactionEditor {
				return testtransaction.NewTestTransaction(
					testtransaction.WithID(txID),
					testtransaction.WithStatus(string(storage.TxStatusSuccess)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithBegun(map[storage.Driver]struct{}{driver: {}}),
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				driver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				driver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(errors.New("finishTx error"))
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "finishTx error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driver := mocks.NewMockDriver(ctrl)

			tt.setupMocks(t, driver)

			err := tt.svc.finishTx(t.Context(), tt.createTx(driver))
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestSaveTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service
		createTx   func(t *testing.T, driver storage.Driver) storage.TransactionEditor
		setupMocks func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver)
		checkTx    func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case: in progress transaction",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: success transaction",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx := testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusSuccess)),
					testtransaction.WithOriginalTx(&storage.Transaction{}),
				)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusSuccess), tx.Status())
			},
			wantErr: require.NoError,
		},
		{
			name: "error creating utility transaction",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				return nil // не создаем транзакцию, чтобы тест прошел ошибку создания utility transaction
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error creating utility transaction")
				require.ErrorContains(t, err, "original transaction not provided")
			}),
		},
		{
			name: "error BuildRequests",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Type().Return(operation.StorageTypeRabbitMQ).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error get builder by storage type")
				require.ErrorContains(t, err, "error building requests for saving transaction")
			}),
		},
		{
			name: "error beginInDriver",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				firstBegin := systemDriver.EXPECT().
					Begin(gomock.Any(), gomock.Any()).
					Return(errors.New("beginInDriver error")).
					Times(1)

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).After(firstBegin).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error beginning transaction")
			}),
		},
		{
			name: "error execRequests",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()

				// // updateTX after fail
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				//
				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("exec error")).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error while executing requests")
				require.ErrorContains(t, err, "error exec request")
			}),
		},
		{
			name: "error saveRequests",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				systemDriver.EXPECT().Type().Return(operation.StorageTypeRabbitMQ).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "unknown storage type: rabbitmq")
				require.ErrorContains(t, err, "error get builder by storage type")
			}),
		},
		{
			name: "error Commit",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) storage.TransactionEditor {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(errors.New("commit error")).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec with rollback")
				require.ErrorContains(t, err, "commit error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driver := mocks.NewMockDriver(ctrl)
			systemDriver := mocks.NewMockDriver(ctrl)

			svc := tt.createSvc(t, systemDriver, driver)

			tt.setupMocks(t, driver, systemDriver)

			tx := tt.createTx(t, driver)

			err := svc.saveTx(t.Context(), tx)
			tt.wantErr(t, err)

			tt.checkTx(t, tx, driver)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestUpdateTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service
		createTx   func(t *testing.T, driver storage.Driver) *storage.Transaction
		setupMocks func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver)
		checkTx    func(t *testing.T, tx *storage.Transaction, driver storage.Driver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) *storage.Transaction {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres)

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil)
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.NoError,
		},
		{
			name: "error creating utility transaction",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) *storage.Transaction {
				t.Helper()

				return nil // не создаем транзакцию, чтобы тест прошел ошибку создания utility transaction
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error creating utility transaction")
				require.ErrorContains(t, err, "original transaction not provided")
			}),
		},
		{
			name: "error BuildRequests",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) *storage.Transaction {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Type().Return(operation.StorageTypeRabbitMQ).AnyTimes()
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error get builder by storage type")
				require.ErrorContains(t, err, "error building requests for updating transaction")
			}),
		},
		{
			name: "error beginInDriver",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) *storage.Transaction {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				firstBegin := systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(errors.New("beginInDriver error")).Times(1)

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).After(firstBegin).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error beginning transaction")
				require.ErrorContains(t, err, "beginInDriver error")
			}),
		},
		{
			name: "error execRequests",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) *storage.Transaction {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				firstExec := systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("exec error")).Times(1)

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).After(firstExec).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error while updating transaction")
				require.ErrorContains(t, err, "error exec request")
			}),
		},
		{
			name: "error Commit",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, driver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, driver)
			},
			createTx: func(t *testing.T, driver storage.Driver) *storage.Transaction {
				t.Helper()

				tx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
				)

				require.NoError(t, err)

				return tx
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				driver.EXPECT().Name().Return("test-storage").AnyTimes()
				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(errors.New("commit error")).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx *storage.Transaction, driver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec with rollback")
				require.ErrorContains(t, err, "commit error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driver := mocks.NewMockDriver(ctrl)
			systemDriver := mocks.NewMockDriver(ctrl)

			svc := tt.createSvc(t, systemDriver, driver)

			tt.setupMocks(t, driver, systemDriver)

			tx := tt.createTx(t, driver)

			err := svc.updateTX(t.Context(), tx)
			tt.wantErr(t, err)

			tt.checkTx(t, tx, driver)
		})
	}
}

func TestFieldsForTx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	userDriver := mocks.NewMockDriver(ctrl)

	err := errors.New("test error")

	txID := random.String(10)

	tx := testtransaction.NewTestTransaction(
		testtransaction.WithStatus(string(storage.TxStatusFailed)),
		testtransaction.WithRequests(map[storage.Driver]*storage.Request{
			userDriver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		}),
		testtransaction.WithFailedDriver(userDriver),
		testtransaction.WithErr(err),
		testtransaction.WithInstanceID(1),
		testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
		testtransaction.WithID(txID),
	)

	svc := &Service{
		instanceID: 1,
		cfg: &operation.Operation{
			Hash: []byte{0x1, 0x2, 0x3},
		},
	}

	expectedFields := map[string]any{
		"id":             txID,
		"status":         string(storage.TxStatusFailed),
		"error":          err.Error(),
		"instance_id":    svc.instanceID,
		"failed_driver":  "test-storage",
		"operation_hash": svc.cfg.Hash,
	}

	userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

	assert.Equal(t, expectedFields, svc.fieldsForTx(tx))
}
