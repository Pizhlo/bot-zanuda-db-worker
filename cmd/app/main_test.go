package main

import (
	"context"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/metrics"
	"db-worker/internal/service/uow"
	"db-worker/internal/service/uow/mocks"
	"db-worker/internal/storage"
	storagemocks "db-worker/internal/storage/mocks"
	"db-worker/internal/storage/model"
	"testing"

	"db-worker/internal/storage/postgres/message"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
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
			mockStorage1 := storagemocks.NewMockDriver(ctrl)
			mockStorage2 := storagemocks.NewMockDriver(ctrl)

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

	mockDriver := storagemocks.NewMockDriver(ctrl)
	mockStorage := storagemocks.NewMockDriver(ctrl)
	mockRequestsRepo := mocks.NewMockrequestsRepo(ctrl)

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

	driversMap := map[string]model.Configurator{
		"test-storage": mockDriver,
	}

	mockDriver.EXPECT().Name().Return("test-storage").Times(1)

	registry := prometheus.NewRegistry()
	metricsService := metrics.New(metrics.WithRegisterer(registry))

	messageRepo := &message.Repo{}

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
		uow.WithRequestsRepo(mockRequestsRepo),
		uow.WithMetricsService(metricsService),
	)
	require.NoError(t, err)

	srv := initOperation(cfg, connection, uowService, messageRepo, driversMap, 1, metricsService, 10)

	require.NotNil(t, srv)
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
