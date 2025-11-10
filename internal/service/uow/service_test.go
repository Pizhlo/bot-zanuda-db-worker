package uow

import (
	"context"
	"db-worker/internal/config/operation"
	builder_pkg "db-worker/internal/service/builder"
	uowmocks "db-worker/internal/service/uow/mocks"
	"db-worker/internal/storage"
	"db-worker/internal/storage/mocks"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
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

func (m *mockStorage) Table() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return "users.users"
}

func (m *mockStorage) DBName() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return "test-db"
}

func (m *mockStorage) Queue() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return ""
}

func (m *mockStorage) RoutingKey() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return ""
}

func (m *mockStorage) InsertTimeout() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return 1000
}

func (m *mockStorage) ReadTimeout() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return 1000
}

func (m *mockStorage) Port() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return 5432
}

func (m *mockStorage) User() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return "user"
}

func (m *mockStorage) Password() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return "password"
}

func (m *mockStorage) Host() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return "localhost"
}

func (m *mockStorage) Name() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.name
}

func (m *mockStorage) Run(_ context.Context) error {
	return nil
}

func (m *mockStorage) Exec(_ context.Context, _ *storage.Request, _ string) error {
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

//nolint:funlen,dupl // много тест-кейсов, схожие тест-кейсы
func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupOpts func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option
		wantErr   require.ErrorAssertionFunc
		want      func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service)
	}{
		{
			name: "positive case",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				mock, ok := driver.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
						},
					}),
					WithStorages([]storage.Driver{mock}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithRequestsRepo(requestsRepo),
					WithMetricsService(metricsService),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()

				// Проверяем только нужные поля, а не всю структуру
				assert.NotNil(t, got)
				assert.NotNil(t, got.cfg)
				assert.Equal(t, "test-storage", got.cfg.Storages[0].Name)
				assert.Equal(t, 1, got.instanceID)
				assert.Equal(t, systemDB, got.storage)
				assert.NotNil(t, got.userStoragesMap)
				assert.NotNil(t, got.userDriversMap)
				assert.Contains(t, got.userStoragesMap, "test-storage")
				assert.Contains(t, got.userDriversMap, "test-storage")
				assert.Equal(t, requestsRepo, got.requestsRepo)
				assert.Equal(t, metricsService, got.metricsService)
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: system storage configs are required",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				mock, ok := driver.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
						},
					}),
					WithStorages([]storage.Driver{mock}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithRequestsRepo(requestsRepo),
					WithMetricsService(metricsService),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: cfg is nil",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				mock, ok := driver.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithStorages([]storage.Driver{driver}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithRequestsRepo(requestsRepo),
					WithMetricsService(metricsService),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storages are required",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
						},
					}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithRequestsRepo(requestsRepo),
					WithMetricsService(metricsService),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storage is required",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				mock, ok := driver.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
						},
					}),
					WithInstanceID(1),
					WithStorages([]storage.Driver{driver}),
					WithRequestsRepo(requestsRepo),
					WithMetricsService(metricsService),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storage not found",
			setupOpts: func(t *testing.T, userStorage storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				systemDB.EXPECT().Name().Return("system-db").AnyTimes()

				mock, ok := userStorage.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
							{Name: "test-storage-2"},
						},
					}),
					WithStorages([]storage.Driver{userStorage}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithRequestsRepo(requestsRepo),
					WithMetricsService(metricsService),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: requests repo is nil",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				mock, ok := driver.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
						},
					}),
					WithStorages([]storage.Driver{mock}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithMetricsService(metricsService),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: requests repo is nil",
			setupOpts: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter) []option {
				t.Helper()

				mock, ok := driver.(*mocks.MockDriver)
				require.True(t, ok)

				mock.EXPECT().Name().Return("test-storage").AnyTimes()

				return []option{
					WithCfg(&operation.Operation{
						Storages: []operation.StorageCfg{
							{Name: "test-storage"},
						},
					}),
					WithStorages([]storage.Driver{mock}),
					WithStorage(systemDB),
					WithInstanceID(1),
					WithRequestsRepo(requestsRepo),
					WithSystemStorageConfigs([]operation.StorageCfg{
						{
							Name:          StorageNameForTransactionsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.transactions",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
						{
							Name:          StorageNameForRequestsTable,
							Type:          operation.StorageTypePostgres,
							Table:         "transactions.requests",
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test-db",
							InsertTimeout: 1000,
							ReadTimeout:   1000,
						},
					}),
				}
			},
			want: func(t *testing.T, driver storage.Driver, systemDB *mocks.MockDriver, requestsRepo *uowmocks.MockrequestsRepo, metricsService *uowmocks.MocktxCounter, got *Service) {
				t.Helper()
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDB := mocks.NewMockDriver(ctrl)
			requestsRepo := uowmocks.NewMockrequestsRepo(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			driver := mocks.NewMockDriver(ctrl)
			opts := tt.setupOpts(t, driver, systemDB, requestsRepo, metricsService)

			got, err := New(opts...)
			tt.wantErr(t, err)

			tt.want(t, driver, systemDB, requestsRepo, metricsService, got)
		})
	}
}

func TestStoragesMap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &Service{
		userDriversMap: map[string]DriversMap{
			"test-storage": {
				driver: &mockStorage{name: "test-storage"},
				cfg: operation.StorageCfg{
					Name: "test-storage",
				},
			},
		},
	}

	got := svc.StoragesMap()
	assert.Equal(t, svc.userDriversMap, got)
}

//nolint:funlen // много тест-кейсов
func TestBuildRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		createSvc       func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) *Service
		msg             map[string]interface{}
		setupMocks      func(t *testing.T, mockDriver1 *mocks.MockDriver, mockDriver2 *mocks.MockDriver)
		setupDriversMap func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[string]DriversMap
		operation       *operation.Operation
		wantErr         require.ErrorAssertionFunc
		setupWant       func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[storage.Driver]*storage.Request
	}{
		{
			name: "positive case: one driver",
			msg: map[string]interface{}{
				"user_id": "1",
			},
			setupMocks: func(t *testing.T, mockDriver1 *mocks.MockDriver, mockDriver2 *mocks.MockDriver) {
				t.Helper()

				mockDriver1.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
			},
			setupDriversMap: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[string]DriversMap {
				t.Helper()

				return map[string]DriversMap{
					"test-storage": {
						driver: driver1,
						cfg: operation.StorageCfg{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
				}
			},
			operation: &operation.Operation{
				Type: operation.OperationTypeCreate,
				Storages: []operation.StorageCfg{
					{
						Name:  "test-storage",
						Table: "users.users",
					},
				},
			},
			createSvc: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Type: operation.OperationTypeCreate,
						Storages: []operation.StorageCfg{
							{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: driver1,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
					},
				}
			},
			wantErr: require.NoError,
			setupWant: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					driver1: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
						Raw:  map[string]any{"user_id": "1"},
					},
				}
			},
		},
		{
			name: "positive case: two drivers",
			msg: map[string]interface{}{
				"user_id": "1",
			},
			setupMocks: func(t *testing.T, mockDriver1 *mocks.MockDriver, mockDriver2 *mocks.MockDriver) {
				t.Helper()

				mockDriver1.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
				mockDriver2.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
			},
			setupDriversMap: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[string]DriversMap {
				t.Helper()

				return map[string]DriversMap{
					"test-storage": {
						driver: driver1,
						cfg: operation.StorageCfg{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
					"test-storage-2": {
						driver: driver2,
						cfg: operation.StorageCfg{
							Name:  "test-storage-2",
							Table: "users.users",
						},
					},
				}
			},
			operation: &operation.Operation{
				Type: operation.OperationTypeCreate,
				Storages: []operation.StorageCfg{
					{Name: "test-storage",
						Type: operation.StorageTypePostgres,
					},
					{Name: "test-storage-2",
						Type: operation.StorageTypePostgres,
					},
				},
			},
			createSvc: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) *Service {
				t.Helper()

				return &Service{
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
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: driver1,
							cfg: operation.StorageCfg{
								Name:  "test-storage",
								Table: "users.users",
							},
						},
						"test-storage-2": {
							driver: driver2,
							cfg: operation.StorageCfg{
								Name:  "test-storage-2",
								Table: "users.users",
							},
						},
					},
				}
			},
			wantErr: require.NoError,
			setupWant: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[storage.Driver]*storage.Request {
				t.Helper()

				return map[storage.Driver]*storage.Request{
					driver1: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
						Raw:  map[string]any{"user_id": "1"},
					},
					driver2: {
						Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
						Args: []any{"1"},
						Raw:  map[string]any{"user_id": "1"},
					},
				}
			},
		},
		{
			name: "negative case: unknown storage type",
			setupMocks: func(t *testing.T, mockDriver1 *mocks.MockDriver, mockDriver2 *mocks.MockDriver) {
				t.Helper()

				mockDriver1.EXPECT().Type().Return(operation.StorageType("unknown")).Times(2)
			},
			msg: map[string]interface{}{
				"user_id": "1",
			},
			operation: &operation.Operation{
				Type: operation.OperationTypeCreate,
				Storages: []operation.StorageCfg{
					{Name: "test-storage", Type: operation.StorageTypePostgres},
					{Name: "test-storage-2", Type: operation.StorageTypePostgres},
				},
			},
			setupDriversMap: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[string]DriversMap {
				t.Helper()

				return map[string]DriversMap{
					"test-storage": {
						driver: driver1,
						cfg: operation.StorageCfg{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
				}
			},
			createSvc: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Type: operation.OperationTypeCreate,
						Storages: []operation.StorageCfg{
							{Name: "test-storage", Type: "unknown"},
						},
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: driver1,
						},
					},
				}
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: unknown operation type",
			setupMocks: func(t *testing.T, mockDriver1 *mocks.MockDriver, mockDriver2 *mocks.MockDriver) {
				t.Helper()

				mockDriver1.EXPECT().Type().Return(operation.StorageTypePostgres).Times(1)
			},
			setupDriversMap: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) map[string]DriversMap {
				t.Helper()

				return map[string]DriversMap{
					"test-storage": {
						driver: driver1,
						cfg: operation.StorageCfg{
							Name:  "test-storage",
							Table: "users.users",
						},
					},
				}
			},
			operation: &operation.Operation{
				Type: "unknown",
				Storages: []operation.StorageCfg{
					{Name: "test-storage",
						Type: operation.StorageTypePostgres,
					},
				},
			},
			createSvc: func(t *testing.T, driver1 storage.Driver, driver2 storage.Driver) *Service {
				t.Helper()

				return &Service{
					cfg: &operation.Operation{
						Type: "unknown",
						Storages: []operation.StorageCfg{
							{Name: "test-storage",
								Type: operation.StorageTypePostgres,
							},
						},
					},
					userDriversMap: map[string]DriversMap{
						"test-storage": {
							driver: &mockStorage{
								name:        "test-storage",
								storageType: operation.StorageTypePostgres,
							},
						},
					},
				}
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driver1 := mocks.NewMockDriver(ctrl)
			driver2 := mocks.NewMockDriver(ctrl)
			svc := tt.createSvc(t, driver1, driver2)

			tt.setupMocks(t, driver1, driver2)

			got, err := svc.BuildRequests(tt.msg, tt.setupDriversMap(t, driver1, driver2), *tt.operation)
			tt.wantErr(t, err)

			if tt.setupWant != nil {
				assert.True(t, assert.ObjectsAreEqual(tt.setupWant(t, driver1, driver2), got))
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

	createBuilder := builder_pkg.ForPostgres().WithCreateOperation()

	updatedBuilder, err := builder_pkg.ForPostgres().WithUpdateOperation()
	require.NoError(t, err)

	deletedBuilder, err := builder_pkg.ForPostgres().WithDeleteOperation()
	require.NoError(t, err)

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
			builder:       builder_pkg.ForPostgres(),
			want:          createBuilder,
			wantErr:       require.NoError,
		},
		{
			name:          "positive case: update",
			operationType: operation.OperationTypeUpdate,
			builder:       builder_pkg.ForPostgres(),
			want:          updatedBuilder,
			wantErr:       require.NoError,
		},
		{
			name:          "positive case: delete",
			operationType: operation.OperationTypeDelete,
			builder:       builder_pkg.ForPostgres(),
			want:          deletedBuilder,
			wantErr:       require.NoError,
		},
		{
			name:          "negative case: unknown operation type",
			operationType: "unknown",
			builder:       builder_pkg.ForPostgres(),
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

//nolint:funlen // много тест-кейсов
func TestMapStoragesConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantErr       require.ErrorAssertionFunc
		driversCfgMap map[string]DriversMap
		storagesCfg   []operation.StorageCfg
		storagesMap   map[string]storage.Driver

		wantDriversMap map[string]DriversMap
	}{
		{
			name: "positive case",
			storagesMap: map[string]storage.Driver{
				"test-storage": &mockStorage{name: "test-storage"},
			},
			driversCfgMap: map[string]DriversMap{
				"test-storage": {
					driver: &mockStorage{name: "test-storage"},
					cfg: operation.StorageCfg{
						Name: "test-storage",
					},
				},
			},
			storagesCfg: []operation.StorageCfg{
				{Name: "test-storage"},
			},
			wantErr: require.NoError,
			wantDriversMap: map[string]DriversMap{
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
			storagesCfg: []operation.StorageCfg{
				{Name: "test-storage"},
				{Name: "test-storage-2"},
			},
			storagesMap: map[string]storage.Driver{
				"test-storage": &mockStorage{name: "test-storage"},
			},
			driversCfgMap: make(map[string]DriversMap),
			wantErr:       require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mapStoragesConfigs(tt.driversCfgMap, tt.storagesCfg, tt.storagesMap)
			tt.wantErr(t, got)

			if tt.wantDriversMap != nil {
				assert.Equal(t, tt.wantDriversMap, tt.driversCfgMap)
			}
		})
	}
}

func newTestService(t *testing.T, systemDriver storage.Driver, userDriver storage.Driver, metricsService *uowmocks.MocktxCounter) *Service {
	t.Helper()

	return &Service{
		metricsService: metricsService,
		instanceID:     1,
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
		userStoragesMap: map[string]storage.Driver{
			"test-storage": userDriver,
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
		},
		requestsDriversMap: map[string]DriversMap{
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
}
