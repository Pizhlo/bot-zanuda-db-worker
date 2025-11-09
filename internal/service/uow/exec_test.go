package uow

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"db-worker/internal/storage/mocks"
	"db-worker/internal/storage/testtransaction"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // много тест-кейсов
func TestExecRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setupSvc func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		rawReq   map[string]any
		requests func(t *testing.T, mock *mocks.MockDriver) map[storage.Driver]*storage.Request
		wantErr  require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			setupSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				// системное хранилище может дергаться многократно в ходе сохранений/обновлений
				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// составляем запросы для сохранения пользовательских запросов, принадлежащих транзакции
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// начать транзакцию в пользовательском хранилище
				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				return newTestService(t, systemDriver, userDriver)
			},
			requests: func(t *testing.T, mock *mocks.MockDriver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					mock: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			rawReq: map[string]any{
				"id": 1,
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: exec error",
			setupSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				// составляем запросы для сохранения пользовательских запросов, принадлежащих транзакции
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// начать транзакцию в пользовательском хранилище
				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error")).AnyTimes()
				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// обновление транзакции в системном хранилище после ошибки
				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
					storage: systemDriver,
				}
			},
			requests: func(t *testing.T, mock *mocks.MockDriver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					mock: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec request: test error")
			}),
		},
		{
			name: "error case: commit error",
			setupSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// составляем запросы для сохранения пользовательских запросов, принадлежащих транзакции
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				// начать транзакцию в пользовательском хранилище
				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(errors.New("test error")).AnyTimes()
				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
						Hash: []byte{0x1, 0x2, 0x3},
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
					storage: systemDriver,
				}
			},
			requests: func(t *testing.T, mock *mocks.MockDriver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					mock: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error commit transaction")
			}),
		},
		{
			name: "error case: finish tx error",
			setupSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// составляем запросы для сохранения пользовательских запросов, принадлежащих транзакции
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				// начать транзакцию в пользовательском хранилище
				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(errors.New("finish tx error")).AnyTimes()

				// ----

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()
				userDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				// составляем запросы для сохранения пользовательских запросов, принадлежащих транзакции
				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				// начать транзакцию в пользовательском хранилище
				userDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(errors.New("test error")).AnyTimes()
				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				userDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				// системный драйвер может вызываться в служебных апдейтах/логировании
				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				return newTestService(t, systemDriver, userDriver)
			},
			requests: func(t *testing.T, mock *mocks.MockDriver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					mock: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "finish tx error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			err := tt.setupSvc(t, systemDriver, userDriver).ExecRequests(t.Context(), tt.requests(t, userDriver), tt.rawReq)
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		createTx   func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor
		wantStatus string
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				originalTx, err := storage.NewTransaction(
					map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
					1,
					[]byte{0x1, 0x2, 0x3},
					map[string]any{
						"id": 1,
					},
				)

				require.NoError(t, err)

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithOriginalTx(originalTx),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantStatus: string(storage.TxStatusSuccess),
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				// commit в пользовательском хранилище
				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil)

				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres)

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil)
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil)

				return newTestService(t, systemDriver, userDriver)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is not in progress",
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusSuccess)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantStatus: string(storage.TxStatusSuccess),
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				return newTestService(t, systemDriver, userDriver)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status not equal to: \"IN_PROGRESS\"")
			}),
		},
		{
			name: "error case: commit error",
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantStatus: string(storage.TxStatusFailed),
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(errors.New("test error"))
				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil)

				systemDriver.EXPECT().Name().Return(StorageNameForTransactionsTable).AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				return newTestService(t, systemDriver, userDriver)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec with rollback: error commit driver: test error")
			}),
		},
		{
			name: "error case: updateTX error",
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantStatus: string(storage.TxStatusSuccess),
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil)

				return newTestService(t, systemDriver, userDriver)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error updating transaction when committing")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			tx := tt.createTx(t, userDriver)

			err := tt.createSvc(t, systemDriver, userDriver).Commit(t.Context(), tx)
			tt.wantErr(t, err)

			assert.Equal(t, tt.wantStatus, tx.Status())
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestRollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createSvc func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		createTx  func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).Times(1)

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusFailed)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.NoError,
		},
		{
			name: "error rollback in driver",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)

				userDriver.EXPECT().Name().Return("test-storage").Times(1)

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusFailed)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is not failed",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status not equal to: \"FAILED\"")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			err := tt.createSvc(t, systemDriver, userDriver).Rollback(t.Context(), tt.createTx(t, userDriver))
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestExecWithTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		createTx   func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor
		wantStatus string
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			wantStatus: string(storage.TxStatusInProgress),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: exec error",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			wantStatus: string(storage.TxStatusFailed),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec request: test error")
			}),
		},
		{
			name: "error: transaction is not in progress",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			wantStatus: string(storage.TxStatusSuccess),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusSuccess)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status not equal to: \"IN_PROGRESS\"")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			tx := tt.createTx(t, userDriver)

			err := tt.createSvc(t, systemDriver, userDriver).execWithTx(t.Context(), tx, userDriver, &storage.Request{
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			})

			tt.wantErr(t, err)

			assert.Equal(t, tt.wantStatus, tx.Status())
		})
	}
}

//nolint:funlen,dupl // длинный тест, похожие тесты
func TestExecWithRollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSvc  func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service
		createTx   func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor
		execFn     func() error
		wantStatus string
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			execFn: func() error {
				return nil
			},
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			wantStatus: string(storage.TxStatusInProgress),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.NoError,
		},
		{
			name: "error: transaction is not in progress",
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
				}
			},
			execFn: func() error {
				return nil
			},
			wantStatus: string(storage.TxStatusSuccess),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusSuccess)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status not equal to: \"IN_PROGRESS\"")
			}),
		},
		{
			name: "error: function error",
			execFn: func() error {
				return errors.New("test error")
			},
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).Times(1)

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
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
					instanceID: 1,
				}
			},
			wantStatus: string(storage.TxStatusFailed),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec with rollback: test error")
			}),
		},
		{
			name: "error: rollback error",
			execFn: func() error {
				return errors.New("test error")
			},
			createSvc: func(t *testing.T, systemDriver *mocks.MockDriver, userDriver *mocks.MockDriver) *Service {
				t.Helper()

				userDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(errors.New("test rollback error")).Times(1)

				userDriver.EXPECT().Name().Return("test-storage").AnyTimes()

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					storage: systemDriver,
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: userDriver,
							cfg: operation.StorageCfg{
								Name: "test-storage",
							},
						},
					},
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
					instanceID: 1,
				}
			},
			wantStatus: string(storage.TxStatusFailed),
			createTx: func(t *testing.T, userDriver *mocks.MockDriver) storage.TransactionEditor {
				t.Helper()

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec with rollback: test error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)

			tx := tt.createTx(t, userDriver)

			err := tt.createSvc(t, systemDriver, userDriver).execWithRollback(t.Context(), tx, userDriver, tt.execFn)
			tt.wantErr(t, err)

			assert.Equal(t, tt.wantStatus, tx.Status())
		})
	}
}
