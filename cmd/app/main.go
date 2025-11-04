package main

import (
	"context"
	handlerV0 "db-worker/internal/api/v0"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	"db-worker/internal/server"
	migration_srv "db-worker/internal/service/migration"
	operation_srv "db-worker/internal/service/operation"
	"db-worker/internal/service/redis"
	"db-worker/internal/service/uow"
	"db-worker/internal/service/worker"
	"db-worker/internal/service/worker/rabbit"
	"db-worker/internal/storage"
	"db-worker/internal/storage/model"
	"db-worker/internal/storage/postgres/message"
	"db-worker/internal/storage/postgres/migration"
	postgres "db-worker/internal/storage/postgres/repo"

	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	_ "db-worker/docs" // swagger docs
)

// @title           DB Worker API
// @version         1.0
// @description     API для работы с базой данных
// @host            localhost:8080
// @basePath        /api/v0

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

	txRepo := initPostgresStorage(notifyCtx, cfg.Storage.Postgres, uow.StorageNameForTransactionsTable, "transactions.transactions OR transactions.requests")

	go butler.start(func() error {
		return txRepo.Run(notifyCtx)
	})
	defer butler.stop(notifyCtx, txRepo)

	messageRepo := initMessageRepo(notifyCtx, cfg.Storage.Postgres)

	go butler.start(func() error {
		return messageRepo.Run(notifyCtx)
	})

	defer butler.stop(notifyCtx, messageRepo)

	operations, err := initOperationServices(cfg, connections, storagesMap, txRepo, messageRepo)
	if err != nil {
		logrus.WithError(err).Fatalf("error initializing operation services")
	}

	for _, operation := range operations {
		go butler.start(func() error {
			return operation.Run(notifyCtx)
		})

		defer butler.stop(notifyCtx, operation)
	}

	migrationRepo := initMigrationRepo(notifyCtx, cfg.Storage.Postgres)
	defer butler.stop(notifyCtx, migrationRepo)

	go butler.start(func() error {
		return migrationRepo.Run(notifyCtx)
	})

	migrationService := initMigrationService(migrationRepo)
	defer butler.stop(notifyCtx, migrationService)

	go butler.start(func() error {
		if err := migrationService.Run(notifyCtx); err != nil {
			logrus.WithError(err).Fatalf("error loading migrations")
		}

		return err
	})

	redis := initRedisStorage(notifyCtx, cfg.Storage.Redis)
	defer butler.stop(notifyCtx, redis)

	handlerV0 := initHandlerV0(butler.BuildInfo)
	server := initServer(handlerV0, cfg.Server)

	go butler.start(func() error {
		return server.Start(notifyCtx)
	})

	// сервер сам закроется при завершении контекста

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

func initHandlerV0(buildInfo *BuildInfo) *handlerV0.Handler {
	logrus.WithFields(logrus.Fields{
		"version":   buildInfo.Version,
		"buildDate": buildInfo.BuildDate,
		"gitCommit": buildInfo.GitCommit,
	}).Info("initializing handler v0")

	return start(
		handlerV0.New(
			handlerV0.WithVersion(buildInfo.Version),
			handlerV0.WithBuildDate(buildInfo.BuildDate),
			handlerV0.WithGitCommit(buildInfo.GitCommit),
		),
	)
}

func initServer(handlerV0 *handlerV0.Handler, cfg config.Server) *server.Server {
	logrus.WithFields(logrus.Fields{
		"port":            cfg.Port,
		"shutdownTimeout": cfg.ShutdownTimeout,
	}).Info("initializing server")

	return start(
		server.New(
			server.WithHandlerV0(handlerV0),
			server.WithPort(cfg.Port),
			server.WithShutdownTimeout(cfg.ShutdownTimeout),
		),
	)
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
	postgresCfg := config.Postgres{
		Host:          storage.Host,
		Port:          storage.Port,
		User:          storage.User,
		Password:      storage.Password,
		DBName:        storage.DBName,
		InsertTimeout: storage.InsertTimeout,
		ReadTimeout:   storage.ReadTimeout,
	}

	name := storage.Name
	table := storage.Table

	switch storage.Type {
	case operation.StorageTypePostgres:
		return initPostgresStorage(ctx, postgresCfg, name, table), nil
	default:
		return nil, fmt.Errorf("unknown storage type: %s", storage.Type)
	}
}

func initPostgresStorage(ctx context.Context, cfg config.Postgres, name string, table string) storage.Driver {
	addr := formatPostgresAddr(cfg)

	logrus.WithFields(logrus.Fields{
		"host":           cfg.Host,
		"port":           cfg.Port,
		"user":           cfg.User,
		"db_name":        cfg.DBName,
		"insert_timeout": cfg.InsertTimeout,
		"read_timeout":   cfg.ReadTimeout,
		"name":           name,
	}).Info("connecting postgres")

	return start(postgres.New(ctx, postgres.WithAddr(addr),
		postgres.WithInsertTimeout(cfg.InsertTimeout),
		postgres.WithReadTimeout(cfg.ReadTimeout),
		postgres.WithName(name),
		postgres.WithCfg(&cfg),
		postgres.WithTable(table),
	))
}

func initMessageRepo(ctx context.Context, cfg config.Postgres) *message.Repo {
	return start(message.New(ctx,
		message.WithAddr(formatPostgresAddr(cfg)),
		message.WithInsertTimeout(cfg.InsertTimeout),
		message.WithReadTimeout(cfg.ReadTimeout),
		message.WithName("messages"),
		message.WithCfg(&cfg),
		message.WithTable("messages.messages"),
	))
}

func initMigrationRepo(ctx context.Context, cfg config.Postgres) *migration.Repo {
	return start(migration.New(ctx,
		migration.WithAddr(formatPostgresAddr(cfg)),
		migration.WithInsertTimeout(cfg.InsertTimeout),
	))
}

func initOperationServices(cfg *config.Config, connections map[string]worker.Worker, storagesMap map[string]storage.Driver, txRepo storage.Driver, messageRepo *message.Repo) (map[string]*operation_srv.Service, error) {
	operations := make(map[string]*operation_srv.Service, len(cfg.Operations.Operations))

	systemStorageConfigs := make([]operation.StorageCfg, 0, 2) // пока что только postgres (transactions.transactions и transactions.requests)

	transactionsPostgresDBCfg := operation.StorageCfg{
		Name:          uow.StorageNameForTransactionsTable,
		Type:          operation.StorageTypePostgres,
		Table:         "transactions.transactions",
		Host:          cfg.Storage.Postgres.Host,
		Port:          cfg.Storage.Postgres.Port,
		User:          cfg.Storage.Postgres.User,
		Password:      cfg.Storage.Postgres.Password,
		DBName:        cfg.Storage.Postgres.DBName,
		InsertTimeout: cfg.Storage.Postgres.InsertTimeout,
		ReadTimeout:   cfg.Storage.Postgres.ReadTimeout,
	}

	requestssPostgresDBCfg := operation.StorageCfg{
		Name:          uow.StorageNameForRequestsTable,
		Type:          operation.StorageTypePostgres,
		Table:         "transactions.requests",
		Host:          cfg.Storage.Postgres.Host,
		Port:          cfg.Storage.Postgres.Port,
		User:          cfg.Storage.Postgres.User,
		Password:      cfg.Storage.Postgres.Password,
		DBName:        cfg.Storage.Postgres.DBName,
		InsertTimeout: cfg.Storage.Postgres.InsertTimeout,
		ReadTimeout:   cfg.Storage.Postgres.ReadTimeout,
	}

	systemStorageConfigs = append(systemStorageConfigs, transactionsPostgresDBCfg, requestssPostgresDBCfg)

	for _, operationCfg := range cfg.Operations.Operations {
		conn, ok := connections[operationCfg.Request.From]
		if !ok {
			return nil, fmt.Errorf("connection %s not found", operationCfg.Request.From)
		}

		storages, err := groupStorages(operationCfg.Storages, storagesMap)
		if err != nil {
			return nil, fmt.Errorf("error grouping storages: %w", err)
		}

		driversMap := make(map[string]model.Configurator, len(storages))
		for _, storage := range storages {
			if _, exists := driversMap[storage.Name()]; exists {
				return nil, fmt.Errorf("duplicate storage name in operation %s: %s", operationCfg.Name, storage.Name())
			}

			driversMap[storage.Name()] = storage
		}

		uow := initUow(storages, &operationCfg, txRepo, systemStorageConfigs, cfg.InstanceID)

		op := initOperation(operationCfg, conn, uow, messageRepo, driversMap, cfg.InstanceID)

		operations[operationCfg.Name] = op
	}

	return operations, nil
}

func initMigrationService(loader *migration.Repo) *migration_srv.Service {
	return start(migration_srv.New(migration_srv.WithMigrationLoader(loader)))
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

func initUow(storages []storage.Driver, operationCfg *operation.Operation, repo storage.Driver, systemStorageConfigs []operation.StorageCfg, instanceID int) *uow.Service {
	return start(uow.New(
		uow.WithStorages(storages),
		uow.WithCfg(operationCfg),
		uow.WithStorage(repo),
		uow.WithSystemStorageConfigs(systemStorageConfigs),
		uow.WithInstanceID(instanceID),
	))
}

func initRedisStorage(ctx context.Context, cfg config.Redis) *redis.Service {
	redis := start(redis.New(redis.WithCfg(&cfg)))

	startService(redis.Connect(ctx), "redis connect")

	return redis
}

func initOperation(operationCfg operation.Operation, connection worker.Worker, uow *uow.Service, messageRepo *message.Repo, driversMap map[string]model.Configurator, instanceID int) *operation_srv.Service {
	op := start(operation_srv.New(
		operation_srv.WithCfg(&operationCfg),
		operation_srv.WithMsgChan(connection.MsgChan()),
		operation_srv.WithUow(uow),
		operation_srv.WithMessageRepo(messageRepo),
		operation_srv.WithDriversMap(driversMap),
		operation_srv.WithInstanceID(instanceID),
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
