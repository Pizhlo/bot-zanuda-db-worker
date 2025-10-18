package uow

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"db-worker/pkg/random"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBeginTx(t *testing.T) {
	t.Parallel()

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
			},
			requests: map[storage.Driver]*storage.Request{
				&mockStorage{name: "test-storage"}: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualTx := tt.svc.beginTx(tt.requests)

			// мы не можем заранее знать какой айди сгенерировался
			assert.NotEmpty(t, actualTx)

			tx, ok := tt.svc.transactions[actualTx.id]
			require.True(t, ok)
			require.NotEmpty(t, tx)

			assert.Equal(t, txStatusInProgress, tx.status)
			assert.Equal(t, tt.requests, tx.requests)
		})
	}
}

//nolint:funlen,dupl // много тест-кейсов, одинаковые тест-кейсы для разных тестов
func TestFinishTx(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	tx := &transaction{
		id:     txID,
		status: txStatusSuccess,
		requests: map[storage.Driver]*storage.Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	tests := []struct {
		name     string
		svc      *Service
		tx       *transaction
		checkMap func(t *testing.T, svc *Service)
		wantErr  require.ErrorAssertionFunc
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
			checkMap: func(t *testing.T, svc *Service) {
				t.Helper()
				assert.Len(t, svc.transactions, 0)
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: transaction is in progress",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test-operation",
				},
				transactions: map[string]*transaction{
					txID: {
						id:     txID,
						status: txStatusInProgress,
						requests: map[storage.Driver]*storage.Request{
							&mockStorage{name: "test-storage"}: {
								Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
								Args: []any{"1"},
							},
						},
					},
				},
			},
			tx: &transaction{
				id:     txID,
				status: txStatusInProgress,
				requests: map[storage.Driver]*storage.Request{
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

			err := tt.svc.finishTx(tt.tx)
			tt.wantErr(t, err)

			if tt.checkMap != nil {
				tt.checkMap(t, tt.svc)
			}
		})
	}
}

func TestSetFailedDriver(t *testing.T) {
	t.Parallel()

	tx := &transaction{
		id:     random.String(10),
		status: txStatusInProgress,
		requests: map[storage.Driver]*storage.Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	driver := "test-driver"

	tx.setFailedDriver(driver)

	assert.Equal(t, driver, tx.failedDriver)
}

//nolint:funlen,dupl // много тест-кейсов, одинаковые тест-кейсы для разных тестов
func TestSetStatus(t *testing.T) {
	t.Parallel()

	tx := &transaction{
		id:     random.String(10),
		status: txStatusInProgress,
		requests: map[storage.Driver]*storage.Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	status := txStatusSuccess

	tx.setStatus(status)

	assert.Equal(t, status, tx.status)
}
