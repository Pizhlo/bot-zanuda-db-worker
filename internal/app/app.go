package app

import (
	"context"
	"db-worker/internal/config"
	message "db-worker/internal/service/message"
	handler "db-worker/internal/service/message/handler"
	interfaces "db-worker/internal/service/message/interface"
	postgres "db-worker/internal/storage/postgres/note_repo"
	"db-worker/internal/storage/postgres/transaction"
	"db-worker/internal/storage/rabbit"
	uow "db-worker/internal/storage/uow/create_notes"
	"fmt"

	"github.com/sirupsen/logrus"
)

type App struct {
	Cfg         *config.Config
	NoteSrv     *message.Service
	NoteStorage *uow.UnitOfWork
	Rabbit      *rabbit.Worker
	TxSaver     *transaction.Repo
}

func NewApp(ctx context.Context, configPath string) (*App, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	setLogLevel(cfg.LogLevel)

	msgChan := make(chan interfaces.Message, cfg.Storage.BufferSize)

	txSaver := initTxSaver(cfg)

	noteStorage := initNoteRepo(cfg, txSaver)

	createNoteHandler := initCreateNoteHandler(noteStorage, cfg.Storage.BufferSize)

	rabbit := initRabbit(ctx, cfg, msgChan)

	noteSrv := initNoteSrv(ctx, createNoteHandler, msgChan)

	return &App{
		Cfg:         cfg,
		NoteSrv:     noteSrv,
		NoteStorage: noteStorage,
		Rabbit:      rabbit,
		TxSaver:     txSaver,
	}, nil
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

func initCreateNoteHandler(storage *uow.UnitOfWork, bufferSize int) interfaces.Handler {
	return start(handler.NewCreateNoteHandler(handler.WithNotesStorage(storage), handler.WithBufferSize(bufferSize)))
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

func initRabbit(ctx context.Context, cfg *config.Config, msgChan chan interfaces.Message) *rabbit.Worker {
	logrus.Infof("connecting rabbit on %s", cfg.Storage.RabbitMQ.Address)

	rabbit := start(rabbit.New(
		rabbit.WithAddress(cfg.Storage.RabbitMQ.Address),
		rabbit.WithNotesTopic(cfg.Storage.RabbitMQ.NoteQueue),
		rabbit.WithSpacesTopic(cfg.Storage.RabbitMQ.SpaceQueue),
		rabbit.WithMsgChan(msgChan),
		rabbit.WithInsertTimeout(cfg.Storage.RabbitMQ.InsertTimeout),
		rabbit.WithReadTimeout(cfg.Storage.RabbitMQ.ReadTimeout),
	))

	startService(rabbit.Connect(), "rabbit")

	go rabbit.HandleNotes(ctx)

	logrus.Infof("successfully connected rabbit on %s", cfg.Storage.RabbitMQ.Address)

	return rabbit
}

func initNoteSrv(ctx context.Context, createNoteHandler interfaces.Handler, msgChan chan interfaces.Message) *message.Service {
	noteSrv := start(message.New(
		message.WithMsgChan(msgChan),
		message.WithCreateHandler(createNoteHandler),
	))

	go noteSrv.Run(ctx)

	return noteSrv
}

func initNoteRepo(cfg *config.Config, txSaver *transaction.Repo) *uow.UnitOfWork {
	addr := formatPostgresAddr(cfg)

	logrus.Infof("connecting db on %s", addr)

	noteStorage := start(postgres.New(postgres.WithAddr(addr),
		postgres.WithInsertTimeout(cfg.Storage.Postgres.InsertTimeout),
		postgres.WithReadTimeout(cfg.Storage.Postgres.ReadTimeout),
	))

	return start(uow.NewUnitOfWork(uow.WithPostgres(noteStorage), uow.WithTxRepo(txSaver)))
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
