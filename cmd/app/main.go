package main

import (
	"context"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	operation_srv "db-worker/internal/service/operation"
	"db-worker/internal/service/redis"
	"db-worker/internal/service/worker"
	"db-worker/internal/service/worker/rabbit"
	"db-worker/internal/storage"
	postgres "db-worker/internal/storage/postgres/repo"
	"db-worker/internal/uow"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

//nolint:funlen // запуск всех сервисов.
func main() {
	ctx := context.Background()

	configPath := flag.String("config", "./config.yaml", "path to config file")
	operationsConfigPath := flag.String("operations-config", "./operations.yaml", "path to operations config file")
	flag.Parse()

	cfg := start(config.LoadConfig(*configPath))

	startService(cfg.LoadOperationConfig(*operationsConfigPath), "operations config")
	startService(cfg.Validate(), "validate config")

	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logrus.WithError(err).Fatalf("error parsing log level")
	}

	logrus.SetLevel(level)

	logrus.WithField("level", logrus.GetLevel()).Info("set log level")

	butler := NewButler()

	logrus.WithFields(logrus.Fields{
		"version": butler.BuildInfo.Version,
		"commit":  butler.BuildInfo.GitCommit,
		"date":    butler.BuildInfo.BuildDate,
	}).Info("starting service")
	defer logrus.Info("shutdown")

	// Создаем контекст для обработки сигналов завершения
	notifyCtx, notify := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	defer notify()

	// запуск воркеров для получения сообщений
	connections, err := initWorkers(cfg)
	if err != nil {
		logrus.WithError(err).Fatalf("error initializing workers")
	}

	for _, worker := range connections {
		go butler.start(func() error {
			startService(worker.Connect(), worker.Name())
			return worker.Run(notifyCtx)
		})

		defer butler.stop(notifyCtx, worker)
	}

	storagesMap, err := initStoragesMap(notifyCtx, cfg)
	if err != nil {
		logrus.WithError(err).Fatalf("error initializing storages map")
	}

	for _, storage := range storagesMap {
		go butler.start(func() error {
			return storage.Run(notifyCtx)
		})

		defer butler.stop(notifyCtx, storage)
	}

	operations, err := initOperationServices(cfg, connections, storagesMap)
	if err != nil {
		logrus.WithError(err).Fatalf("error initializing operation services")
	}

	for _, operation := range operations {
		go butler.start(func() error {
			return operation.Run(notifyCtx)
		})

		defer butler.stop(notifyCtx, operation)
	}

	redis := initRedisStorage(notifyCtx, cfg.Storage.Redis)
	defer butler.stop(notifyCtx, redis)

	logrus.Info("all services started")

	// Ждем сигнал завершения
	<-notifyCtx.Done()
	logrus.Info("received shutdown signal, stopping services...")

	// Ждем завершения всех горутин
	butler.waitForAll()
	logrus.Info("all services stopped")
}

// создает подключения из списка подключений. Сохраняет в map[string]*rabbit.Worker.
func initWorkers(cfg *config.Config) (map[string]worker.Worker, error) {
	connections := make(map[string]worker.Worker)

	var err error
	for _, connection := range cfg.Operations.Connections {
		connections[connection.Name], err = initWorker(connection)
		if err != nil {
			return nil, fmt.Errorf("error initializing worker %s: %w", connection.Name, err)
		}
	}

	return connections, nil
}

func initWorker(worker operation.Connection) (worker.Worker, error) {
	switch worker.Type {
	case operation.ConnectionTypeRabbitMQ:
		return initRabbit(worker), nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", worker.Type)
	}
}

// initRabbit создает подключение для исполнения отдельной операции.
func initRabbit(connection operation.Connection) worker.Worker {
	logrus.WithFields(logrus.Fields{
		"address":        connection.Address,
		"name":           connection.Name,
		"exchange":       connection.Queue,
		"routing_key":    connection.RoutingKey,
		"insert_timeout": connection.InsertTimeout,
		"read_timeout":   connection.ReadTimeout,
	}).Info("connecting rabbit")

	rabbit := start(rabbit.New(
		rabbit.WithAddress(connection.Address),
		rabbit.WithName(connection.Name),
		rabbit.WithExchange("exchange"),
		rabbit.WithQueue(connection.Queue),
		rabbit.WithRoutingKey(connection.RoutingKey),
		rabbit.WithInsertTimeout(connection.InsertTimeout),
		rabbit.WithReadTimeout(connection.ReadTimeout),
	))

	return rabbit
}

func initStoragesMap(ctx context.Context, cfg *config.Config) (map[string]storage.Driver, error) {
	storagesMap := make(map[string]storage.Driver)

	var err error
	for _, storage := range cfg.Operations.Storages {
		storagesMap[storage.Name], err = initStorage(ctx, storage)
		if err != nil {
			return nil, fmt.Errorf("error initializing storage %s: %w", storage.Name, err)
		}
	}

	return storagesMap, nil
}

func initStorage(ctx context.Context, storage operation.StorageCfg) (storage.Driver, error) {
	switch storage.Type {
	case operation.StorageTypePostgres:
		return initPostgresStorage(ctx, storage), nil
	default:
		return nil, fmt.Errorf("unknown storage type: %s", storage.Type)
	}
}

func initPostgresStorage(ctx context.Context, cfg operation.StorageCfg) storage.Driver {
	addr := formatPostgresAddr(config.Postgres{
		Host:          cfg.Host,
		Port:          cfg.Port,
		User:          cfg.User,
		Password:      cfg.Password,
		DBName:        cfg.DBName,
		InsertTimeout: cfg.InsertTimeout,
		ReadTimeout:   cfg.ReadTimeout,
	})

	logrus.WithFields(logrus.Fields{
		"host":           cfg.Host,
		"port":           cfg.Port,
		"user":           cfg.User,
		"db_name":        cfg.DBName,
		"insert_timeout": cfg.InsertTimeout,
		"read_timeout":   cfg.ReadTimeout,
		"name":           cfg.Name,
	}).Info("connecting postgres")

	return start(postgres.New(ctx, postgres.WithAddr(addr),
		postgres.WithInsertTimeout(cfg.InsertTimeout),
		postgres.WithReadTimeout(cfg.ReadTimeout),
		postgres.WithInsertTimeout(cfg.InsertTimeout),
		postgres.WithReadTimeout(cfg.ReadTimeout),
		postgres.WithName(cfg.Name),
	))
}

func initOperationServices(cfg *config.Config, connections map[string]worker.Worker, storagesMap map[string]storage.Driver) (map[string]*operation_srv.Service, error) {
	operations := make(map[string]*operation_srv.Service, len(cfg.Operations.Operations))

	for _, operationCfg := range cfg.Operations.Operations {
		conn, ok := connections[operationCfg.Request.From]
		if !ok {
			return nil, fmt.Errorf("connection %s not found", operationCfg.Request.From)
		}

		storages, err := groupStorages(operationCfg.Storages, storagesMap)
		if err != nil {
			return nil, fmt.Errorf("error grouping storages: %w", err)
		}

		uow := initUow(storages, &operationCfg)

		op := initOperation(operationCfg, conn, uow)

		operations[operationCfg.Name] = op
	}

	return operations, nil
}

func groupStorages(storagesCfg []operation.StorageCfg, storagesMap map[string]storage.Driver) ([]storage.Driver, error) {
	storages := make([]storage.Driver, len(storagesCfg))

	for i, storageCfg := range storagesCfg {
		storage, ok := storagesMap[storageCfg.Name]
		if !ok {
			return nil, fmt.Errorf("storage %s not found", storageCfg.Name)
		}

		storages[i] = storage
	}

	return storages, nil
}

func initUow(storages []storage.Driver, operationCfg *operation.Operation) *uow.Service {
	return start(uow.New(
		uow.WithStorages(storages),
		uow.WithCfg(operationCfg),
	))
}

func initRedisStorage(ctx context.Context, cfg config.Redis) *redis.Service {
	redis := start(redis.New(redis.WithCfg(&cfg)))

	startService(redis.Connect(ctx), "redis connect")

	return redis
}

func initOperation(operationCfg operation.Operation, connection worker.Worker, uow *uow.Service) *operation_srv.Service {
	op := start(operation_srv.New(
		operation_srv.WithCfg(&operationCfg),
		operation_srv.WithMsgChan(connection.MsgChan()),
		operation_srv.WithUow(uow),
	))

	return op
}

func startService(err error, name string) {
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"service": name,
		}).Fatalf("error creating service: %+v", err)
	}
}

func start[T any](svc T, err error) T {
	startService(err, fmt.Sprintf("%T", svc))

	return svc
}

func formatPostgresAddr(cfg config.Postgres) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password,
		cfg.Host, cfg.Port, cfg.DBName)
}
