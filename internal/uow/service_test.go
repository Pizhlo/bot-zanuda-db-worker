package uow

import (
	"context"
	"db-worker/internal/config/operation"
	builder_pkg "db-worker/internal/service/builder"
	"db-worker/internal/storage"
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

	execCalled bool
	rolledBack bool
	mu         sync.Mutex

	execError   error
	commitError error
}

func (m *mockStorage) Name() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.name
}

func (m *mockStorage) Run(_ context.Context) error {
	return nil
}

func (m *mockStorage) getExecCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.execCalled
}

func (m *mockStorage) Exec(_ context.Context, _ *storage.Request) error {
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

func (m *mockStorage) Begin(ctx context.Context, _ string) error {
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

func (m *mockStorage) Commit(ctx context.Context, _ string) error {
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

//nolint:funlen // много тест-кейсов
func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []option
		wantErr require.ErrorAssertionFunc
		want    *Service
	}{
		{
			name: "positive case",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithStorages([]storage.Driver{&mockStorage{name: "test-storage"}}),
			},
			want: &Service{
				cfg: &operation.Operation{},
				storagesMap: map[string]storage.Driver{
					"test-storage": &mockStorage{name: "test-storage"},
				},
				transactions: make(map[string]*transaction),
				driversMap: map[string]drivers{
					"test-storage": {
						driver: &mockStorage{name: "test-storage"},
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: cfg is nil",
			opts: []option{
				WithStorages([]storage.Driver{&mockStorage{name: "test-storage"}}),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storages are required",
			opts: []option{
				WithCfg(&operation.Operation{}),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storage not found",
			opts: []option{
				WithCfg(&operation.Operation{
					Storages: []operation.StorageCfg{
						{Name: "test-storage"},
						{Name: "test-storage-2"},
					},
				}),
				WithStorages([]storage.Driver{&mockStorage{name: "test-storage"}}),
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(tt.opts...)
			tt.wantErr(t, err)

			if got != nil {
				assert.ObjectsAreEqual(tt.want, got)
			}
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestMapStorages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		svc        *Service
		wantErr    require.ErrorAssertionFunc
		driversMap map[string]drivers
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Storages: []operation.StorageCfg{
						{Name: "test-storage"},
					},
				},
				storagesMap: map[string]storage.Driver{
					"test-storage": &mockStorage{name: "test-storage"},
				},
				transactions: make(map[string]*transaction),
				driversMap: map[string]drivers{
					"test-storage": {
						driver: &mockStorage{name: "test-storage"},
						cfg: operation.StorageCfg{
							Name: "test-storage",
						},
					},
				},
			},
			wantErr: require.NoError,
			driversMap: map[string]drivers{
				"test-storage": {
					driver: &mockStorage{name: "test-storage"},
					cfg: operation.StorageCfg{
						Name: "test-storage",
					},
				},
			},
		},
		{
			name: "negative case: storage not found",
			svc: &Service{
				cfg: &operation.Operation{
					Storages: []operation.StorageCfg{
						{Name: "test-storage"},
						{Name: "test-storage-2"},
					},
				},
				storagesMap: map[string]storage.Driver{
					"test-storage": &mockStorage{name: "test-storage"},
				},
				transactions: make(map[string]*transaction),
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.svc.mapStorages()
			tt.wantErr(t, got)

			if tt.driversMap != nil {
				assert.Equal(t, tt.driversMap, tt.svc.driversMap)
			}
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestBuildRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     *Service
		msg     map[string]interface{}
		wantErr require.ErrorAssertionFunc
		want    map[storage.Driver]*storage.Request
	}{
		{
			name: "positive case: one driver",
			msg: map[string]interface{}{
				"user_id": "1",
			},
			svc: &Service{
				cfg: &operation.Operation{
					Type: operation.OperationTypeCreate,
					Storages: []operation.StorageCfg{
						{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
				},
				driversMap: map[string]drivers{
					"test-storage": {
						driver: &mockStorage{
							name:        "test-storage",
							storageType: operation.StorageTypePostgres,
						},
						cfg: operation.StorageCfg{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
				},
			},
			wantErr: require.NoError,
			want: map[storage.Driver]*storage.Request{
				&mockStorage{name: "test-storage"}: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
		{
			name: "positive case: two drivers",
			msg: map[string]interface{}{
				"user_id": "1",
			},
			svc: &Service{
				cfg: &operation.Operation{
					Type: operation.OperationTypeCreate,
					Storages: []operation.StorageCfg{
						{
							Name:  "test-storage",
							Table: "users.users",
						},
						{
							Name:  "test-storage-2",
							Table: "users.users",
						},
					},
				},
				driversMap: map[string]drivers{
					"test-storage": {
						driver: &mockStorage{
							name:        "test-storage",
							storageType: operation.StorageTypePostgres,
						},
						cfg: operation.StorageCfg{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
					"test-storage-2": {
						driver: &mockStorage{
							name:        "test-storage-2",
							storageType: operation.StorageTypePostgres,
						},
						cfg: operation.StorageCfg{
							Name:  "test-storage-2",
							Table: "users.users",
						},
					},
				},
			},
			wantErr: require.NoError,
			want: map[storage.Driver]*storage.Request{
				&mockStorage{name: "test-storage"}: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
				&mockStorage{name: "test-storage-2"}: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
		{
			name: "negative case: unknown storage type",
			svc: &Service{
				cfg: &operation.Operation{
					Type: operation.OperationTypeCreate,
					Storages: []operation.StorageCfg{
						{Name: "test-storage", Type: "unknown"},
					},
				},
				driversMap: map[string]drivers{
					"test-storage": {
						driver: &mockStorage{
							name:        "test-storage",
							storageType: "unknown",
						},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: unknown operation type",
			svc: &Service{
				cfg: &operation.Operation{
					Type: "unknown",
					Storages: []operation.StorageCfg{
						{Name: "test-storage",
							Type: operation.StorageTypePostgres,
						},
					},
				},
				driversMap: map[string]drivers{
					"test-storage": {
						driver: &mockStorage{
							name:        "test-storage",
							storageType: operation.StorageTypePostgres,
						},
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.svc.BuildRequests(tt.msg)
			tt.wantErr(t, err)

			if tt.want != nil {
				assert.ObjectsAreEqual(tt.want, got)
			}
		})
	}
}

func TestBuilderByStorageType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		storageType operation.StorageType
		want        builder_pkg.Builder
		wantErr     require.ErrorAssertionFunc
	}{
		{
			name:        "positive case: postgres",
			storageType: operation.StorageTypePostgres,
			want:        builder_pkg.ForPostgres(),
			wantErr:     require.NoError,
		},
		{
			name:        "negative case: unknown storage type",
			storageType: "unknown",
			want:        nil,
			wantErr:     require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := builderByStorageType(tt.storageType)
			tt.wantErr(t, err)

			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSetOperationType(t *testing.T) {
	t.Parallel()

	updatedBuilder, err := builder_pkg.ForPostgres().WithUpdateOperation()
	require.NoError(t, err)

	postgresBuilder := builder_pkg.ForPostgres()

	tests := []struct {
		name          string
		operationType operation.Type
		builder       builder_pkg.Builder
		want          builder_pkg.Builder
		wantErr       require.ErrorAssertionFunc
	}{
		{
			name:          "positive case: create",
			operationType: operation.OperationTypeCreate,
			builder:       postgresBuilder,
			want:          postgresBuilder.WithCreateOperation(),
			wantErr:       require.NoError,
		},
		{
			name:          "positive case: update",
			operationType: operation.OperationTypeUpdate,
			builder:       updatedBuilder,
			want:          updatedBuilder,
			wantErr:       require.NoError,
		},
		{
			name:          "negative case: unknown operation type",
			operationType: "unknown",
			builder:       postgresBuilder,
			want:          nil,
			wantErr:       require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := setOperationType(tt.builder, tt.operationType)
			tt.wantErr(t, err)

			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
