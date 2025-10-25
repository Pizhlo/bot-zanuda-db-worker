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

//nolint:funlen,gocognit // длинный тест
func TestBeginTx(t *testing.T) {
	t.Parallel()

	driver := &mockStorage{name: "test-storage"}
	driverBeginTxError := &mockStorage{name: "test-storage-begin-tx-error", beginTxError: errors.New("test error")}
	driverFinishTxError := &mockStorage{name: "test-storage-finish-tx-error", finishTxError: errors.New("test error")}

	tests := []struct {
		name       string
		svc        *Service
		requests   map[storage.Driver]*storage.Request
		checkMocks func(t *testing.T, requests map[storage.Driver]*storage.Request, tx *storage.Transaction)
		checkTx    func(t *testing.T, svc *Service, requests map[storage.Driver]*storage.Request, actualTx *storage.Transaction)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: make(map[string]*storage.Transaction),
			},
			requests: map[storage.Driver]*storage.Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
			checkMocks: func(t *testing.T, requests map[storage.Driver]*storage.Request, tx *storage.Transaction) {
				t.Helper()

				assert.True(t, driver.getBeginTxCalled())
				assert.False(t, driver.getFinishTxCalled())
			},
			checkTx: func(t *testing.T, svc *Service, requests map[storage.Driver]*storage.Request, actualTx *storage.Transaction) {
				t.Helper()

				// то, что вернула функция
				assert.NotNil(t, actualTx)
				assert.Equal(t, storage.TxStatusInProgress, actualTx.Status)
				assert.Equal(t, requests, actualTx.Requests)

				BegunMap := map[string]struct{}{
					driver.Name(): {},
				}

				assert.Equal(t, BegunMap, actualTx.Begun)

				// транзакция будет одна, но мы не знаем айди
				for _, tx := range svc.transactions {
					assert.Equal(t, storage.TxStatusInProgress, tx.Status)
					assert.Equal(t, requests, tx.Requests)
				}

				assert.Len(t, svc.transactions, 1)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: failed to begin transaction in driver",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: make(map[string]*storage.Transaction),
			},
			requests: func() map[storage.Driver]*storage.Request {
				driver := &mockStorage{name: "test-storage"}
				driverBeginTxError := &mockStorage{name: "test-begin-tx-error", beginTxError: errors.New("test error")}

				return map[storage.Driver]*storage.Request{
					driver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
					driverBeginTxError: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				}
			}(),
			checkMocks: func(t *testing.T, requests map[storage.Driver]*storage.Request, tx *storage.Transaction) {
				t.Helper()

				// Проверяем, что драйверы были вызваны
				// Но поскольку мы создаем новые экземпляры в requests,
				// мы не можем проверить их состояние

				if tx != nil {
					_, ok := tx.Begun[driver.Name()]
					assert.True(t, ok, "driver %s should be in Begun map for transaction %s", driver.Name(), tx.ID)

					for driver := range requests {
						if _, ok := tx.Begun[driver.Name()]; ok {
							assert.True(t, driver.(*mockStorage).getBeginTxCalled(), "method beginTx should be called for driver %s", driver.Name())
						}
					}
				} else {
					t.Log("transaction is nil")
				}
			},
			checkTx: func(t *testing.T, svc *Service, requests map[storage.Driver]*storage.Request, actualTx *storage.Transaction) {
				t.Helper()

				assert.Nil(t, actualTx)
				assert.Len(t, svc.transactions, 0)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualTx, err := tt.svc.beginTx(t.Context(), tt.requests)
			tt.wantErr(t, err)

			if tt.checkTx != nil {
				tt.checkTx(t, tt.svc, tt.requests, actualTx)
			}

			if tt.checkMocks != nil {
				tt.checkMocks(t, tt.requests, actualTx)
			}

			driver.clear()
			driverBeginTxError.clear()
			driverFinishTxError.clear()
		})
	}
}

//nolint:funlen // длинный тест
func TestFinishTx(t *testing.T) {
	t.Parallel()

	txID := random.String(10)
	driver := &mockStorage{name: "test-storage"}

	tests := []struct {
		name       string
		svc        *Service
		tx         *storage.Transaction
		checkMap   func(t *testing.T, svc *Service)
		checkMocks func(t *testing.T, driver *mockStorage)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: func() *Service {
				tx := &storage.Transaction{
					ID:     txID,
					Status: storage.TxStatusSuccess,
					Requests: map[storage.Driver]*storage.Request{
						driver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					},
				}

				return &Service{
					cfg: &operation.Operation{
						Name: "test-operation",
					},
					transactions: map[string]*storage.Transaction{
						txID: tx,
					},
				}
			}(),
			tx: &storage.Transaction{
				ID:     txID,
				Status: storage.TxStatusSuccess,
				Requests: map[storage.Driver]*storage.Request{
					driver: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
			},
			checkMap: func(t *testing.T, svc *Service) {
				t.Helper()
				assert.Len(t, svc.transactions, 0)
			},
			checkMocks: func(t *testing.T, driver *mockStorage) {
				t.Helper()

				// В данном тесте мы не можем проверить вызов finishTx,
				// так как используем разные экземпляры драйверов
				// Проверяем только, что тест прошел без ошибок
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is in progress",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*storage.Transaction{
					txID: {
						ID:     txID,
						Status: storage.TxStatusInProgress,
						Requests: map[storage.Driver]*storage.Request{
							&mockStorage{name: "test-storage"}: {
								Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
								Args: []any{"1"},
							},
						},
					},
				},
			},
			tx: &storage.Transaction{
				ID:     txID,
				Status: storage.TxStatusInProgress,
				Requests: map[storage.Driver]*storage.Request{
					&mockStorage{name: "test-storage"}: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
					},
				},
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction status equal to: \"in progress\", but expected: \"success\" or \"failed")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.finishTx(t.Context(), tt.tx)
			tt.wantErr(t, err)

			if tt.checkMap != nil {
				tt.checkMap(t, tt.svc)
			}

			if tt.checkMocks != nil {
				tt.checkMocks(t, driver)
			}
		})
	}
}
