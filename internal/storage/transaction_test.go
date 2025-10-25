package storage

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/pkg/random"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStorage struct {
	name        string
	storageType operation.StorageType
	timeout     int // для искусственной задержки

	execCalled     bool
	rolledBack     bool
	commitCalled   bool
	finishTxCalled bool
	beginTxCalled  bool

	mu sync.Mutex

	execError     error
	commitError   error
	finishTxError error
	beginTxError  error
}

func (m *mockStorage) Name() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.name
}

func (m *mockStorage) Run(_ context.Context) error {
	return nil
}

func (m *mockStorage) Exec(_ context.Context, _ *Request, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.execCalled = true

	return m.execError
}

func (m *mockStorage) Stop(_ context.Context) error {
	return nil
}

func (m *mockStorage) Type() operation.StorageType {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.storageType
}

//nolint:dupl // одинаковая логика в моках
func (m *mockStorage) Begin(ctx context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.beginTxCalled = true

	if m.timeout > 0 {
		// Создаем канал для отслеживания завершения сна
		done := make(chan struct{})

		go func() {
			time.Sleep(time.Duration(m.timeout) * time.Millisecond)
			close(done)
		}()

		// Ждем либо завершения сна, либо отмены контекста
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return m.beginTxError
}

//nolint:dupl // одинаковая логика в моках
func (m *mockStorage) Commit(ctx context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.commitCalled = true

	if m.timeout > 0 {
		// Создаем канал для отслеживания завершения сна
		done := make(chan struct{})

		go func() {
			time.Sleep(time.Duration(m.timeout) * time.Millisecond)
			close(done)
		}()

		// Ждем либо завершения сна, либо отмены контекста
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return m.commitError
}

func (m *mockStorage) Rollback(ctx context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.rolledBack = true

	if m.timeout > 0 {
		// Создаем канал для отслеживания завершения сна
		done := make(chan struct{})

		go func() {
			time.Sleep(time.Duration(m.timeout) * time.Millisecond)
			close(done)
		}()

		// Ждем либо завершения сна, либо отмены контекста
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (m *mockStorage) FinishTx(ctx context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.finishTxCalled = true

	return m.finishTxError
}

func TestNewTransaction(t *testing.T) {
	t.Parallel()

	mock := &mockStorage{
		name:        "test-db",
		storageType: operation.StorageTypePostgres,
	}

	tests := []struct {
		name          string
		status        txStatus
		requests      map[Driver]*Request
		instanceID    int
		operationHash []byte
		want          *Transaction
		wantError     require.ErrorAssertionFunc
		checkWant     func(want, got *Transaction)
	}{
		{
			name:   "positive case",
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				mock: {
					Val:  "insert into users.users (id, name) values ($1, $2)",
					Args: []any{1, "ivan"},
					Raw: map[string]any{
						"id":   1,
						"name": "ivan",
					},
				},
			},
			instanceID:    1,
			operationHash: []byte{0x1, 0x2, 0x3},
			want: &Transaction{
				Status: TxStatusInProgress,
				Requests: map[Driver]*Request{
					mock: {
						Val:  "insert into users.users (id, name) values ($1, $2)",
						Args: []any{1, "ivan"},
						Raw: map[string]any{
							"id":   1,
							"name": "ivan",
						},
					},
				},
				InstanceID:    1,
				OperationHash: []byte{0x1, 0x2, 0x3},
			},
			checkWant: func(want, got *Transaction) {
				assert.Equal(t, want.Status, got.Status)
				assert.EqualValues(t, want.Requests, got.Requests)
				assert.Equal(t, want.InstanceID, got.InstanceID)
				assert.Equal(t, want.OperationHash, got.OperationHash)

				assert.Len(t, got.Begun, 0)
				assert.Len(t, got.ID, 10)
			},
			wantError: require.NoError,
		},
		{
			name: "status not provided",
			requests: map[Driver]*Request{
				mock: {
					Val:  "insert into users.users (id, name) values ($1, $2)",
					Args: []any{1, "ivan"},
					Raw: map[string]any{
						"id":   1,
						"name": "ivan",
					},
				},
			},
			instanceID:    1,
			operationHash: []byte{0x1, 0x2, 0x3},
			wantError:     require.Error,
		},
		{
			name:          "requests not provided",
			status:        TxStatusInProgress,
			instanceID:    1,
			operationHash: []byte{0x1, 0x2, 0x3},
			wantError:     require.Error,
		},
		{
			name:       "requests not provided",
			status:     TxStatusInProgress,
			instanceID: 1,
			requests: map[Driver]*Request{
				mock: {
					Val:  "insert into users.users (id, name) values ($1, $2)",
					Args: []any{1, "ivan"},
					Raw: map[string]any{
						"id":   1,
						"name": "ivan",
					},
				},
			},
			wantError: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx, err := NewTransaction(tt.status, tt.requests, tt.instanceID, tt.operationHash)
			tt.wantError(t, err)

			if tt.checkWant != nil {
				tt.checkWant(tt.want, tx)
			}
		})
	}
}

func TestSetFailedDriver(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		ID:     random.String(10),
		Status: TxStatusInProgress,
		Requests: map[Driver]*Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	driver := "test-driver"

	tx.SetFailedDriver(driver)

	assert.Equal(t, driver, tx.FailedDriver)
}

func TestSetStatus(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		ID:     random.String(10),
		Status: TxStatusInProgress,
		Requests: map[Driver]*Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	Status := TxStatusSuccess

	tx.SetStatus(Status)

	assert.Equal(t, Status, tx.Status)
}

func TestSetFailedStatus(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		ID:     random.String(10),
		Status: TxStatusInProgress,
		Requests: map[Driver]*Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	driver := "test-driver"
	err := errors.New("test error")

	tx.SetFailedStatus(driver, err)

	assert.Equal(t, driver, tx.FailedDriver)
	assert.Equal(t, TxStatusFailed, tx.Status)
	assert.Equal(t, err, tx.Err)
}

func TestSetSuccessStatus(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		ID:     random.String(10),
		Status: TxStatusInProgress,
		Requests: map[Driver]*Request{
			&mockStorage{name: "test-storage"}: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	tx.SetSuccessStatus()

	assert.Equal(t, TxStatusSuccess, tx.Status)
}

func TestIsInProgress(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		Status: TxStatusInProgress,
	}

	assert.True(t, tx.IsInProgress())
}

func TestIsFailed(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		Status: TxStatusFailed,
	}

	assert.True(t, tx.IsFailed())
}

//nolint:funlen // длинный тест
func TestIsEqualStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tx     *Transaction
		Status txStatus
		want   assert.BoolAssertionFunc
	}{
		{
			name: "equal to success",
			tx: &Transaction{
				Status: TxStatusSuccess,
			},
			Status: TxStatusSuccess,
			want:   assert.True,
		},
		{
			name: "equal to failed",
			tx: &Transaction{
				Status: TxStatusFailed,
			},
			Status: TxStatusFailed,
			want:   assert.True,
		},
		{
			name: "equal to in progress",
			tx: &Transaction{
				Status: TxStatusInProgress,
			},
			Status: TxStatusInProgress,
			want:   assert.True,
		},
		{
			name: "not equal to success",
			tx: &Transaction{
				Status: TxStatusInProgress,
			},
			Status: TxStatusSuccess,
			want:   assert.False,
		},
		{
			name: "not equal to failed",
			tx: &Transaction{
				Status: TxStatusInProgress,
			},
			Status: TxStatusFailed,
			want:   assert.False,
		},
		{
			name: "not equal to in progress",
			tx: &Transaction{
				Status: TxStatusSuccess,
			},
			Status: TxStatusInProgress,
			want:   assert.False,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.want(t, tt.tx.isEqualStatus(tt.Status))
		})
	}
}
