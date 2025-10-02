package app

import (
	"context"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	operation_srv "db-worker/internal/service/operation"
	"db-worker/internal/service/worker"
	"db-worker/internal/service/worker/rabbit"
	storage "db-worker/internal/storage"
	postgres "db-worker/internal/storage/postgres/repo"
	"db-worker/internal/storage/postgres/transaction"
	"fmt"

	"github.com/sirupsen/logrus"
)

type App struct {
	Cfg        *config.Config
	Workers    map[string]worker.Worker
	Storages   map[string]storage.StorageDriver
	Operations map[string]*operation_srv.OperationService
	TxSaver    *transaction.Repo
}

func NewApp(ctx context.Context, configPath string, operationConfigPath string) (*App, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	// Устанавливаем уровень логирования сразу после загрузки основного конфига
	setLogLevel(cfg.LogLevel)

	err = cfg.LoadOperationConfig(operationConfigPath)
	if err != nil {
		return nil, fmt.Errorf("error loading operation config: %w", err)
	}

	err = cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("error validating config: %w", err)
	}

	txSaver := initTxSaver(cfg)

	connections, err := initWorkers(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error initializing workers: %w", err)
	}

	storagesMap, err := initStoragesMap(cfg)
	if err != nil {
		return nil, fmt.Errorf("error initializing storages map: %w", err)
	}

	operations, err := initOperationServices(ctx, cfg, connections, storagesMap)
	if err != nil {
		return nil, fmt.Errorf("error initializing operation services: %w", err)
	}

	return &App{
		Cfg:        cfg,
		TxSaver:    txSaver,
		Workers:    connections,
		Storages:   storagesMap,
		Operations: operations,
	}, nil
}

func initTxSaver(cfg *config.Config) *transaction.Repo {
	addr := formatPostgresAddr(cfg.Storage.Postgres)

	txSaver := start(transaction.New(transaction.WithAddr(addr),
		transaction.WithInsertTimeout(cfg.Storage.Postgres.InsertTimeout),
		transaction.WithReadTimeout(cfg.Storage.Postgres.ReadTimeout),
		transaction.WithInstanceID(cfg.InstanceID),
	))

	return txSaver
}

func setLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logrus.Fatalf("error parsing log level: %+v", err)
	}

	logrus.SetLevel(lvl)

	logrus.Infof("set log level: %+v", logrus.GetLevel())
}

// создает подключения из списка подключений. Сохраняет в map[string]*rabbit.Worker
func initWorkers(ctx context.Context, cfg *config.Config) (map[string]worker.Worker, error) {
	connections := make(map[string]worker.Worker)

	var err error
	for _, connection := range cfg.Operations.Connections {
		connections[connection.Name], err = initWorker(ctx, connection)
		if err != nil {
			return nil, fmt.Errorf("error initializing worker %s: %w", connection.Name, err)
		}
	}

	return connections, nil
}

func initWorker(ctx context.Context, worker operation.Connection) (worker.Worker, error) {
	switch worker.Type {
	case operation.ConnectionTypeRabbitMQ:
		return initRabbit(ctx, worker), nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", worker.Type)
	}
}

func initStoragesMap(cfg *config.Config) (map[string]storage.StorageDriver, error) {
	storagesMap := make(map[string]storage.StorageDriver)

	var err error
	for _, storage := range cfg.Operations.Storages {
		storagesMap[storage.Name], err = initStorage(storage)
		if err != nil {
			return nil, fmt.Errorf("error initializing storage %s: %w", storage.Name, err)
		}
	}

	return storagesMap, nil
}

func initStorage(storage operation.Storage) (storage.StorageDriver, error) {
	switch storage.Type {
	case operation.StorageTypePostgres:
		return initPostgresStorage(config.Postgres{
			Host:          storage.Host,
			Port:          storage.Port,
			User:          storage.User,
			Password:      storage.Password,
			DBName:        storage.DBName,
			InsertTimeout: storage.InsertTimeout,
			ReadTimeout:   storage.ReadTimeout,
		}), nil
	default:
		return nil, fmt.Errorf("unknown storage type: %s", storage.Type)
	}
}

func initPostgresStorage(cfg config.Postgres) storage.StorageDriver {
	addr := formatPostgresAddr(cfg)

	return start(postgres.New(postgres.WithAddr(addr),
		postgres.WithInsertTimeout(cfg.InsertTimeout),
		postgres.WithReadTimeout(cfg.ReadTimeout),
		postgres.WithInsertTimeout(cfg.InsertTimeout),
		postgres.WithReadTimeout(cfg.ReadTimeout),
	))
}

// initRabbit создает подключение для исполнения отдельной операции.
func initRabbit(ctx context.Context, connection operation.Connection) worker.Worker {
	logrus.Infof("connecting rabbit on %s", connection.Address)

	rabbit := start(rabbit.New(
		rabbit.WithAddress(connection.Address),
		rabbit.WithName(connection.Name),
		rabbit.WithExchange(connection.Queue),
		rabbit.WithRoutingKey(connection.RoutingKey),
		rabbit.WithInsertTimeout(connection.InsertTimeout),
		rabbit.WithReadTimeout(connection.ReadTimeout),
	))

	startService(rabbit.Connect(), "rabbit")

	go func() {
		if err := rabbit.Run(ctx); err != nil {
			logrus.Fatalf("error running rabbit %s: %+v", connection.Name, err)
		}
	}()

	return rabbit
}

func initOperationServices(ctx context.Context, cfg *config.Config, connections map[string]worker.Worker, storagesMap map[string]storage.StorageDriver) (map[string]*operation_srv.OperationService, error) {
	operations := make(map[string]*operation_srv.OperationService, len(cfg.Operations.Operations))

	for _, operationCfg := range cfg.Operations.Operations {
		conn, ok := connections[operationCfg.Request.From]
		if !ok {
			return nil, fmt.Errorf("connection %s not found", operationCfg.Request.From)
		}

		storages, err := groupStorages(operationCfg.Storages, storagesMap)
		if err != nil {
			return nil, fmt.Errorf("error grouping storages: %w", err)
		}

		op := initOperation(ctx, operationCfg, conn, storages)

		operations[operationCfg.Name] = op
	}

	return operations, nil
}

func groupStorages(storagesCfg []operation.Storage, storagesMap map[string]storage.StorageDriver) ([]storage.StorageDriver, error) {
	storages := make([]storage.StorageDriver, len(storagesCfg))

	for i, storageCfg := range storagesCfg {
		storage, ok := storagesMap[storageCfg.Name]
		if !ok {
			return nil, fmt.Errorf("storage %s not found", storageCfg.Name)
		}

		storages[i] = storage
	}

	return storages, nil
}

func initOperation(ctx context.Context, operationCfg operation.Operation, connection worker.Worker, storages []storage.StorageDriver) *operation_srv.OperationService {
	op := start(operation_srv.New(
		operation_srv.WithCfg(&operationCfg),
		operation_srv.WithConnection(connection),
		operation_srv.WithStorages(storages),
		operation_srv.WithMsgChan(connection.MsgChan()),
	))

	go func() {
		if err := op.Run(ctx); err != nil {
			logrus.Fatalf("error running operation %s: %+v", operationCfg.Name, err)
		}
	}()

	return op
}

func formatPostgresAddr(cfg config.Postgres) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password,
		cfg.Host, cfg.Port, cfg.DBName)
}

func startService(err error, name string) {
	if err != nil {
		logrus.Fatalf("error creating %s: %+v", name, err)
	}
}

func start[T any](svc T, err error) T {
	startService(err, fmt.Sprintf("%T", svc))

	return svc
}
