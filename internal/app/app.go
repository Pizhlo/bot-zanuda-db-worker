package app

import (
	"context"
	"db-worker/internal/config"
	model_config "db-worker/internal/config/model"
	message "db-worker/internal/service/message"
	handler "db-worker/internal/service/message/handler"
	interfaces "db-worker/internal/service/message/interface"
	postgres "db-worker/internal/storage/postgres/note_repo"
	"db-worker/internal/storage/postgres/transaction"
	"db-worker/internal/storage/rabbit"
	create_notes "db-worker/internal/storage/uow/create_notes"
	update_notes "db-worker/internal/storage/uow/update_notes"
	"fmt"

	"github.com/sirupsen/logrus"
)

type App struct {
	Cfg         *config.Config
	MsgService  *message.Service
	TxSaver     *transaction.Repo
	Connections []connection
}

type connection interface {
	Close()
}

func NewApp(ctx context.Context, configPath string, modelConfigPath string) (*App, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	// Устанавливаем уровень логирования сразу после загрузки основного конфига
	setLogLevel(cfg.LogLevel)

	modelCfg, err := model_config.LoadModelConfig(modelConfigPath)
	if err != nil {
		return nil, fmt.Errorf("error loading model config: %w", err)
	}

	txSaver := initTxSaver(cfg)

	app := &App{
		Cfg:         cfg,
		TxSaver:     txSaver,
		Connections: make([]connection, 0),
	}

	storage := initStorage(cfg)

	createStorage := initNoteCreator(txSaver, storage)
	updateStorage := initNoteUpdater(txSaver, storage)

	createChannels := make([]chan interfaces.Message, 0)
	updateChannels := make([]chan interfaces.Message, 0)
	createHandler := initCreateNoteHandler(createStorage, cfg.Storage.BufferSize)
	updateHandler := initUpdateNoteHandler(updateStorage, cfg.Storage.BufferSize)

	for _, model := range modelCfg.Models {
		for _, operation := range model.Operations {
			switch operation.Request.Connection.Type {
			case model_config.RabbitMQRequestType:
				handler, err := operation.Request.GetRequestHandler()
				if err != nil {
					return nil, fmt.Errorf("error getting request handler: %w", err)
				}

				msgChan := make(chan interfaces.Message, cfg.Storage.BufferSize)

				app.Connections = append(app.Connections,
					initRabbit(ctx, operation.Request.Connection.Address, handler.GetTopic(), cfg.Storage.RabbitMQ.InsertTimeout, cfg.Storage.RabbitMQ.ReadTimeout, msgChan, operation.Fields, operation.Type))

				saveChannel(msgChan, &createChannels, &updateChannels, operation.Type)
			case model_config.HTTPRequestType:
				logrus.Warnf("http request is not supported yet")
			default:
				return nil, fmt.Errorf("unknown request type: %s", operation.Request.Connection.Type)
			}

		}
	}

	messageSrv := initMessageSrv(ctx, createHandler, updateHandler, createChannels, updateChannels)

	app.MsgService = messageSrv

	return app, nil
}

func saveChannel(ch chan interfaces.Message, createChannels *[]chan interfaces.Message, updateChannels *[]chan interfaces.Message, operation string) {
	if operation == model_config.OperationTypeCreate {
		*createChannels = append(*createChannels, ch)
	} else {
		*updateChannels = append(*updateChannels, ch)
	}
}

func initTxSaver(cfg *config.Config) *transaction.Repo {
	addr := formatPostgresAddr(cfg)

	txSaver := start(transaction.New(transaction.WithAddr(addr),
		transaction.WithInsertTimeout(cfg.Storage.Postgres.InsertTimeout),
		transaction.WithReadTimeout(cfg.Storage.Postgres.ReadTimeout),
		transaction.WithInstanceID(cfg.InstanceID),
	))

	return txSaver
}

func initCreateNoteHandler(storage *create_notes.UnitOfWork, bufferSize int) interfaces.Handler {
	return start(handler.NewCreateHandler(handler.WithNotesCreator(storage), handler.WithBufferSizeCreateHandler(bufferSize)))
}

func initUpdateNoteHandler(storage *update_notes.UnitOfWork, bufferSize int) interfaces.Handler {
	return start(handler.NewUpdateHandler(handler.WithNotesUpdater(storage), handler.WithBufferSizeUpdateHandler(bufferSize)))
}

func setLogLevel(level string) {
	switch level {
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}

	logrus.Infof("log level: %+v", logrus.GetLevel())
}

func initRabbit(ctx context.Context, addr string, topic string, insertTimeout int, readTimeout int, msgChan chan interfaces.Message, fields map[string]model_config.Field, operation string) *rabbit.Worker {
	logrus.Infof("connecting rabbit on %s", addr)

	rabbit := start(rabbit.New(
		rabbit.WithAddress(addr),
		rabbit.WithMsgChan(msgChan),
		rabbit.WithInsertTimeout(insertTimeout),
		rabbit.WithReadTimeout(readTimeout),
		rabbit.WithFields(fields),
		rabbit.WithOperation(operation),
	))

	startService(rabbit.Connect(topic), "rabbit")

	go rabbit.HandleTopic(ctx)

	logrus.Infof("successfully connected rabbit on %s", addr)

	return rabbit
}

func initMessageSrv(ctx context.Context, createHandler interfaces.Handler, updateHandler interfaces.Handler, createChannels []chan interfaces.Message, updateChannels []chan interfaces.Message) *message.Service {
	messageSrv := start(message.New(
		message.WithCreateChannels(createChannels),
		message.WithUpdateChannels(updateChannels),
		message.WithCreateHandler(createHandler),
		message.WithUpdateHandler(updateHandler),
	))

	go messageSrv.Run(ctx)

	return messageSrv
}

func initStorage(cfg *config.Config) *postgres.Repo {
	addr := formatPostgresAddr(cfg)

	logrus.Infof("connecting db on %s", addr)

	return start(postgres.New(postgres.WithAddr(addr),
		postgres.WithInsertTimeout(cfg.Storage.Postgres.InsertTimeout),
		postgres.WithReadTimeout(cfg.Storage.Postgres.ReadTimeout),
	))
}

func initNoteCreator(txSaver *transaction.Repo, noteStorage *postgres.Repo) *create_notes.UnitOfWork {
	return start(create_notes.NewUnitOfWork(create_notes.WithPostgres(noteStorage), create_notes.WithTxRepo(txSaver)))
}

func initNoteUpdater(txSaver *transaction.Repo, noteStorage *postgres.Repo) *update_notes.UnitOfWork {
	return start(update_notes.NewUnitOfWork(update_notes.WithPostgres(noteStorage), update_notes.WithTxRepo(txSaver)))
}

func formatPostgresAddr(cfg *config.Config) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Storage.Postgres.User, cfg.Storage.Postgres.Password,
		cfg.Storage.Postgres.Host, cfg.Storage.Postgres.Port, cfg.Storage.Postgres.DBName)
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
