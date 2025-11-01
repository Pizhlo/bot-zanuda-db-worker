package main

import (
	"context"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/uow"
	"db-worker/internal/service/worker"
	"db-worker/internal/storage"
	"db-worker/internal/storage/mocks"
	"testing"

	postgres "db-worker/internal/storage/postgres/repo"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatPostgresAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      config.Postgres
		expected string
	}{
		{
			name: "valid postgres config",
			cfg: config.Postgres{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				DBName:   "testdb",
			},
			expected: "postgresql://testuser:testpass@localhost:5432/testdb?sslmode=disable",
		},
		{
			name: "postgres config with special characters in password",
			cfg: config.Postgres{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "test@pass#123",
				DBName:   "testdb",
			},
			expected: "postgresql://testuser:test@pass#123@localhost:5432/testdb?sslmode=disable",
		},
		{
			name: "postgres config with different port",
			cfg: config.Postgres{
				Host:     "192.168.1.100",
				Port:     5433,
				User:     "admin",
				Password: "admin123",
				DBName:   "production",
			},
			expected: "postgresql://admin:admin123@192.168.1.100:5433/production?sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := formatPostgresAddr(tt.cfg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

//nolint:funlen // это тест
func TestGroupStorages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		storagesCfg []operation.StorageCfg
		expectError bool
		errorMsg    string
		expectedLen int
	}{
		{
			name: "valid storages",
			storagesCfg: []operation.StorageCfg{
				{Name: "storage1"},
				{Name: "storage2"},
			},
			expectError: false,
			expectedLen: 2,
		},
		{
			name: "single storage",
			storagesCfg: []operation.StorageCfg{
				{Name: "storage1"},
			},
			expectError: false,
			expectedLen: 1,
		},
		{
			name: "storage not found",
			storagesCfg: []operation.StorageCfg{
				{Name: "nonexistent"},
			},
			expectError: true,
			errorMsg:    "storage nonexistent not found",
		},
		{
			name:        "empty storages",
			storagesCfg: []operation.StorageCfg{},
			expectError: false,
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Создаем мок storages
			mockStorage1 := mocks.NewMockDriver(ctrl)
			mockStorage2 := mocks.NewMockDriver(ctrl)

			storagesMap := map[string]storage.Driver{
				"storage1": mockStorage1,
				"storage2": mockStorage2,
			}

			storages, err := groupStorages(tt.storagesCfg, storagesMap)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, storages)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, storages)
				assert.Len(t, storages, tt.expectedLen)
			}
		})
	}
}

type mockWorker struct {
	name          string
	address       string
	queue         string
	routingKey    string
	insertTimeout int
	readTimeout   int
	msgChan       chan map[string]interface{}
}

func (m *mockWorker) Name() string {
	return m.name
}

func (m *mockWorker) Run(ctx context.Context) error {
	return nil
}

func (m *mockWorker) Stop(ctx context.Context) error {
	return nil
}

func (m *mockWorker) MsgChan() chan map[string]interface{} {
	return m.msgChan
}

func (m *mockWorker) Connect() error {
	return nil
}

func (m *mockWorker) Address() string {
	return m.address
}

func (m *mockWorker) Queue() string {
	return m.queue
}

func (m *mockWorker) RoutingKey() string {
	return m.routingKey
}

func (m *mockWorker) InsertTimeout() int {
	return m.insertTimeout
}

func (m *mockWorker) ReadTimeout() int {
	return m.readTimeout
}

func TestInitOperation(t *testing.T) {
	t.Parallel()

	cfg := operation.Operation{
		Name: "test-operation",
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mocks.NewMockDriver(ctrl)
	mockStorage := mocks.NewMockDriver(ctrl)

	connection := &mockWorker{
		name:          "test-connection",
		msgChan:       make(chan map[string]interface{}),
		address:       "test-address",
		queue:         "test-queue",
		routingKey:    "test-routing-key",
		insertTimeout: 1000,
		readTimeout:   1000,
	}

	storages := []storage.Driver{
		mockDriver,
	}

	mockDriver.EXPECT().Name().Return("test-storage").Times(1)

	uowService, err := uow.New(
		uow.WithStorages(storages),
		uow.WithCfg(&cfg),
		uow.WithStorage(mockStorage),
		uow.WithSystemStorageConfigs([]operation.StorageCfg{
			{
				Name:  uow.StorageNameForRequestsTable,
				Type:  operation.StorageTypePostgres,
				Table: "test-table",
			},
		}),
	)
	require.NoError(t, err)

	srv := initOperation(cfg, connection, uowService)

	require.NotNil(t, srv)
}

//nolint:funlen // много тест-кейсов
func TestInitOperationServices(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage1 := mocks.NewMockDriver(ctrl)
	mockStorage2 := mocks.NewMockDriver(ctrl)
	mockStorage3 := mocks.NewMockDriver(ctrl)
	mockStorage4 := mocks.NewMockDriver(ctrl)

	cfg := &config.Config{
		Operations: operation.OperationConfig{
			Operations: []operation.Operation{
				{
					Name: "test-operation-1",
					Storages: []operation.StorageCfg{
						{Name: "test-storage-1"},
						{Name: "test-storage-2"},
					},
					Request: operation.Request{
						From: "test-connection-1"},
				},
				{
					Name: "test-operation-2",
					Storages: []operation.StorageCfg{
						{Name: "test-storage-3"},
						{Name: "test-storage-4"},
					},
					Request: operation.Request{
						From: "test-connection-2",
					},
				},
			},
		},
	}

	connection1 := &mockWorker{name: "test-connection-1", msgChan: make(chan map[string]interface{})}
	connection2 := &mockWorker{name: "test-connection-2", msgChan: make(chan map[string]interface{})}

	connections := map[string]worker.Worker{
		"test-connection-1": connection1,
		"test-connection-2": connection2,
	}

	storagesMap := map[string]storage.Driver{
		"test-storage-1": mockStorage1,
		"test-storage-2": mockStorage2,
		"test-storage-3": mockStorage3,
		"test-storage-4": mockStorage4,
	}

	mockStorage1.EXPECT().Name().Return("test-storage-1").Times(1)
	mockStorage2.EXPECT().Name().Return("test-storage-2").Times(1)
	mockStorage3.EXPECT().Name().Return("test-storage-3").Times(1)
	mockStorage4.EXPECT().Name().Return("test-storage-4").Times(1)

	opts := []postgres.RepoOption{
		postgres.WithAddr("test-addr"),
		postgres.WithInsertTimeout(1000),
		postgres.WithReadTimeout(1000),
		postgres.WithCfg(&config.Postgres{
			Host:     "test-host",
			Port:     5432,
			User:     "test-user",
			Password: "test-password",
			DBName:   "test-db",
		}),
		postgres.WithTable("test-table"),
		postgres.WithName("test connection"),
	}

	repo, err := postgres.New(context.Background(), opts...)
	require.NoError(t, err)

	services, err := initOperationServices(cfg, connections, storagesMap, repo)
	require.NoError(t, err)
	require.NotNil(t, services)

	assert.Equal(t, len(cfg.Operations.Operations), len(services))
}

func TestInitRabbit(t *testing.T) {
	t.Parallel()

	cfg := operation.Connection{
		Name:          "test-connection",
		Address:       "test-address",
		Queue:         "test-queue",
		RoutingKey:    "test-routing-key",
		InsertTimeout: 1000,
		ReadTimeout:   1000,
	}

	rabbit := initRabbit(cfg)
	require.NotNil(t, rabbit)

	assert.Equal(t, cfg.Name, rabbit.Name())
	assert.Equal(t, cfg.Address, rabbit.Address())
	assert.Equal(t, cfg.Queue, rabbit.Queue())
	assert.Equal(t, cfg.RoutingKey, rabbit.RoutingKey())
	assert.Equal(t, cfg.InsertTimeout, rabbit.InsertTimeout())
	assert.Equal(t, cfg.ReadTimeout, rabbit.ReadTimeout())
}

//nolint:funlen // это тест
func TestInitWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         *config.Config
		expectError bool
		errorMsg    string
		expectedLen int
	}{
		{
			name: "valid multiple workers",
			cfg: &config.Config{
				Operations: operation.OperationConfig{
					Connections: []operation.Connection{
						{
							Name:          "worker1",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://localhost:5672",
							Queue:         "queue1",
							RoutingKey:    "key1",
							InsertTimeout: 30,
							ReadTimeout:   30,
						},
						{
							Name:          "worker2",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://localhost:5672",
							Queue:         "queue2",
							RoutingKey:    "key2",
							InsertTimeout: 45,
							ReadTimeout:   45,
						},
						{
							Name:          "worker3",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://localhost:5672",
							Queue:         "queue3",
							RoutingKey:    "key3",
							InsertTimeout: 60,
							ReadTimeout:   60,
						},
					},
				},
			},
			expectError: false,
			expectedLen: 3,
		},
		{
			name: "single worker",
			cfg: &config.Config{
				Operations: operation.OperationConfig{
					Connections: []operation.Connection{
						{
							Name:          "single-worker",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://localhost:5672",
							Queue:         "single-queue",
							RoutingKey:    "single-key",
							InsertTimeout: 30,
							ReadTimeout:   30,
						},
					},
				},
			},
			expectError: false,
			expectedLen: 1,
		},
		{
			name: "unknown worker type",
			cfg: &config.Config{
				Operations: operation.OperationConfig{
					Connections: []operation.Connection{
						{
							Name:          "invalid-worker",
							Type:          "unknown-type",
							Address:       "amqp://localhost:5672",
							Queue:         "queue",
							RoutingKey:    "key",
							InsertTimeout: 30,
							ReadTimeout:   30,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "unknown worker type: unknown-type",
		},
		{
			name: "mixed valid and invalid workers",
			cfg: &config.Config{
				Operations: operation.OperationConfig{
					Connections: []operation.Connection{
						{
							Name:          "valid-worker",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://localhost:5672",
							Queue:         "valid-queue",
							RoutingKey:    "valid-key",
							InsertTimeout: 30,
							ReadTimeout:   30,
						},
						{
							Name:          "invalid-worker",
							Type:          "unknown-type",
							Address:       "amqp://localhost:5672",
							Queue:         "invalid-queue",
							RoutingKey:    "invalid-key",
							InsertTimeout: 30,
							ReadTimeout:   30,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "unknown worker type: unknown-type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workers, err := initWorkers(tt.cfg)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, workers)
			} else {
				require.NoError(t, err)
				require.NotNil(t, workers)

				assert.Len(t, workers, tt.expectedLen)

				// Проверяем, что все воркеры созданы с правильными именами
				for _, conn := range tt.cfg.Operations.Connections {
					worker, exists := workers[conn.Name]
					assert.True(t, exists, "worker for connection %s should exist", conn.Name)

					require.NotNil(t, worker, "worker should not be nil")
					assert.Equal(t, conn.Name, worker.Name())
				}

				// Проверяем, что в мапе нет лишних воркеров
				assert.Len(t, workers, len(tt.cfg.Operations.Connections))
			}
		})
	}
}
