package uow

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"db-worker/pkg/random"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen,dupl // много тест-кейсов, одинаковые тест-кейсы для разных тестов
func TestCommit(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	// Создаем один экземпляр mockStorage для использования в тесте
	mockStorageInstance := &mockStorage{name: "test-storage"}

	tx := &transaction{
		id:     txID,
		status: txStatusInProgress,
		requests: map[storage.Driver]*storage.Request{
			mockStorageInstance: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
		failedDriver: "",
	}

	tests := []struct {
		name    string
		svc     *Service
		tx      *transaction
		wantTx  *transaction
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: tx,
				},
			},
			tx: tx,
			wantTx: &transaction{
				id:     txID,
				status: txStatusSuccess,
				requests: map[storage.Driver]*storage.Request{
					mockStorageInstance: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is not in progress",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: {
						id:       txID,
						status:   txStatusFailed,
						requests: map[storage.Driver]*storage.Request{},
					},
				},
			},
			tx: &transaction{
				id:       txID,
				status:   txStatusFailed,
				requests: map[storage.Driver]*storage.Request{},
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status not equal to: \"in progress\"")
			}),
		},
		{
			name: "error case: context deadline exceeded",
			svc: &Service{
				cfg: &operation.Operation{
					Name:    "test-operation",
					Timeout: 10, // 10ms timeout для операции
				},
				driversMap: map[string]drivers{
					"test-driver": {
						driver: &mockStorage{name: "test-driver", timeout: 50}, // 50ms задержка, больше чем timeout операции
						cfg: operation.StorageCfg{
							Name: "test-driver",
						},
					},
				},
			},
			tx: &transaction{
				id:     txID,
				status: txStatusInProgress,
				requests: map[storage.Driver]*storage.Request{
					&mockStorage{name: "test-storage", timeout: 50}: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
			},

			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "context deadline exceeded")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.Commit(t.Context(), tt.tx)
			tt.wantErr(t, err)

			if tt.wantTx != nil {
				assert.EqualValues(t, tt.wantTx, tt.tx)
			}
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, одинаковые тест-кейсы для разных тестов
func TestRollback(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	driversMap := map[string]drivers{
		"test-driver-1": {
			driver: &mockStorage{name: "test-driver-1"},
			cfg: operation.StorageCfg{
				Name: "test-driver-1",
			},
		},
		"test-driver-2": {
			driver: &mockStorage{name: "test-driver-2"},
			cfg: operation.StorageCfg{
				Name: "test-driver-2",
			},
		},
	}

	tx := &transaction{
		id:           txID,
		status:       txStatusFailed,
		requests:     map[storage.Driver]*storage.Request{},
		failedDriver: "test-driver-1",
	}

	tests := []struct {
		name       string
		svc        *Service
		id         string
		tx         *transaction
		wantTx     *transaction
		checkMocks func(t *testing.T, svc *Service)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: tx,
				},
				driversMap: driversMap,
			},
			tx: tx,
			checkMocks: func(t *testing.T, svc *Service) {
				t.Helper()

				assert.False(t, svc.driversMap["test-driver-1"].driver.(*mockStorage).rolledBack)
				assert.True(t, svc.driversMap["test-driver-2"].driver.(*mockStorage).rolledBack)

				tx, ok := svc.transactions[txID]
				require.False(t, ok)

				assert.Nil(t, tx)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is not failed",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: {
						id:       txID,
						status:   txStatusInProgress,
						requests: map[storage.Driver]*storage.Request{},
					},
				},
			},
			tx: &transaction{
				id:       txID,
				status:   txStatusInProgress,
				requests: map[storage.Driver]*storage.Request{},
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status not equal to: \"failed\"")
			}),
		},
		{
			name: "error case: context deadline exceeded",
			svc: &Service{
				cfg: &operation.Operation{
					Name:    "test-operation",
					Timeout: 10, // 10ms timeout для операции
				},
				transactions: map[string]*transaction{
					txID: {
						id:       txID,
						status:   txStatusFailed,
						requests: map[storage.Driver]*storage.Request{},
					},
				},
				driversMap: map[string]drivers{
					"test-driver": {
						driver: &mockStorage{name: "test-driver", timeout: 50}, // 50ms задержка, больше чем timeout операции
						cfg: operation.StorageCfg{
							Name: "test-driver",
						},
					},
				},
			},
			tx: &transaction{
				id:     txID,
				status: txStatusFailed,
				requests: map[storage.Driver]*storage.Request{
					&mockStorage{name: "test-driver", timeout: 50}: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
			},
			id: txID,
			checkMocks: func(t *testing.T, svc *Service) {
				t.Helper()

				assert.True(t, svc.driversMap["test-driver"].driver.(*mockStorage).rolledBack)

				tx, ok := svc.transactions[txID]
				require.False(t, ok)
				assert.Nil(t, tx)
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.Rollback(t.Context(), tt.tx)
			tt.wantErr(t, err)

			if tt.wantTx != nil {
				assert.Equal(t, tt.wantTx, tt.tx)
			}

			if tt.checkMocks != nil {
				tt.checkMocks(t, tt.svc)
			}
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestExecRequests(t *testing.T) {
	t.Parallel()

	driver := &mockStorage{name: "test-storage"}
	driverExecError := &mockStorage{name: "test-storage", execError: errors.New("test error")}
	driverCommitError := &mockStorage{name: "test-storage", commitError: errors.New("test error")}

	tests := []struct {
		name     string
		svc      *Service
		requests map[storage.Driver]*storage.Request
		wantErr  require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: make(map[string]*transaction),
				driversMap: map[string]drivers{
					"test-storage": {
						driver: driver,
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			requests: map[storage.Driver]*storage.Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: exec error",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: make(map[string]*transaction),
				driversMap: map[string]drivers{
					"test-storage": {
						driver: driverExecError,
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			requests: map[storage.Driver]*storage.Request{
				driverExecError: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec request: test error")
			}),
		},
		{
			name: "error case: commit error",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: make(map[string]*transaction),
				driversMap: map[string]drivers{
					"test-storage": {
						driver: driverCommitError,
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			requests: map[storage.Driver]*storage.Request{
				driverCommitError: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "test error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.ExecRequests(t.Context(), tt.requests)
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestExecWithTx(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	driver := &mockStorage{name: "test-storage"}

	driverExecError := &mockStorage{name: "test-storage", execError: errors.New("test error")}

	tx := &transaction{
		id:     txID,
		status: txStatusInProgress,
		requests: map[storage.Driver]*storage.Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
		failedDriver: "",
	}

	tests := []struct {
		name    string
		svc     *Service
		tx      *transaction
		driver  storage.Driver
		req     *storage.Request
		wantTx  transaction
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: tx,
				},
				driversMap: map[string]drivers{
					"test-storage": {
						driver: driver,
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			tx:     tx,
			driver: driver,
			req: &storage.Request{
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
			wantTx: transaction{
				id:     txID,
				status: txStatusInProgress,
				requests: map[storage.Driver]*storage.Request{
					driver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
				failedDriver: "",
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: exec error",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: tx,
				},
				driversMap: map[string]drivers{
					"test-storage": {
						driver: driverExecError,
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			tx:     tx,
			driver: driverExecError,
			req: &storage.Request{
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
			wantTx: transaction{
				id:     txID,
				status: txStatusFailed,
				requests: map[storage.Driver]*storage.Request{
					driver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
				failedDriver: "test-storage",
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error exec request: test error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.execWithTx(t.Context(), tt.tx, tt.driver, tt.req)
			tt.wantErr(t, err)

			tx, ok := tt.svc.transactions[tt.tx.id]

			// можем проверить транзакцию только если она существует
			if tt.wantTx.id != "" {
				require.True(t, ok)
				require.NotEmpty(t, tx)

				assert.Equal(t, &tt.wantTx, tx)
			}

			assert.True(t, tt.driver.(*mockStorage).getExecCalled())
		})
	}
}

func TestExecWithRollback(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	tests := []struct {
		name    string
		svc     *Service
		tx      *transaction
		fn      func() error
		req     *storage.Request
		wantTx  transaction
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
			},
			tx: &transaction{
				id:       txID,
				status:   txStatusInProgress,
				requests: map[storage.Driver]*storage.Request{},
			},
			fn:      func() error { return nil },
			wantErr: require.NoError,
		},
		{
			name: "error case: fn error",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: {
						id:       txID,
						status:   txStatusInProgress,
						requests: map[storage.Driver]*storage.Request{},
					},
				},
			},
			tx: &transaction{
				id:       txID,
				status:   txStatusInProgress,
				requests: map[storage.Driver]*storage.Request{},
			},
			fn: func() error { return errors.New("test error") },
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "test error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.execWithRollback(t.Context(), tt.tx, tt.fn)
			tt.wantErr(t, err)
		})
	}
}
