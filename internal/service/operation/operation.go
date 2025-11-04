package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/service/operation/message"
	"db-worker/internal/service/uow"
	"db-worker/internal/storage"
	"db-worker/internal/storage/model"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// Service - сервис для выполнения операций.
type Service struct {
	cfg        *operation.Operation // конфигурация операции
	instanceID int

	mu sync.Mutex

	uow unitOfWork

	messageRepo    messageRepo    // репозиторий для работы с сообщениями
	metricsService messageCounter // сервис для работы с метриками

	msgChan  chan map[string]interface{}
	quitChan chan struct{}

	messages map[uuid.UUID]*message.Message

	driversMap map[string]model.Configurator
}

// unitOfWork инкапсулирует логику работы с хранилищами. Берет на себя построение запросов и выполнение их.
//
//go:generate mockgen -source=operation.go -destination=mocks/mocks.go -package=mocks
type unitOfWork interface {
	BuildRequests(msg map[string]interface{}, driversMap map[string]uow.DriversMap, operation operation.Operation) (map[storage.Driver]*storage.Request, error)
	ExecRequests(ctx context.Context, requests map[storage.Driver]*storage.Request) error
	StoragesMap() map[string]uow.DriversMap
}

type messageRepo interface {
	messageCreator
	messageUpdater
	messageGetter
}

type messageCreator interface {
	CreateMany(ctx context.Context, messages []message.Message) error
}

type messageUpdater interface {
	UpdateMany(ctx context.Context, messages []message.Message) error
}

type messageGetter interface {
	Get(ctx context.Context, id string) (message.Message, error)
	GetAll(ctx context.Context) ([]message.Message, error)
}

type messageCounter interface {
	messageAdder
	messageDecrementer
}

type messageAdder interface {
	// AddProcessingMessages добавляет количество сообщений в статусе in progress.
	AddProcessingMessages(count int)
	// AddFailedMessages добавляет количество сообщений в статусе failed.
	AddFailedMessages(count int)
	// AddValidatedMessages добавляет количество сообщений в статусе validated.
	AddValidatedMessages(count int)
	// AddTotalMessages добавляет общее количество сообщений (обработанные + не обработанные).
	AddTotalMessages(count int)
	// AddProcessedMessages добавляет количество обработанных сообщений.
	AddProcessedMessages(count int)
}

type messageDecrementer interface {
	// DecrementProcessingMessagesBy уменьшает количество сообщений в процессе обработки на count.
	DecrementProcessingMessagesBy(count int)
	// DecrementFailedMessagesBy уменьшает количество сообщений в статусе failed на count.
	DecrementFailedMessagesBy(count int)
}

// option определяет опции для сервиса.
type option func(*Service)

// WithCfg устанавливает конфигурацию операции.
func WithCfg(cfg *operation.Operation) option {
	return func(s *Service) {
		s.cfg = cfg
	}
}

// WithMsgChan устанавливает канал для получения сообщений.
func WithMsgChan(msgChan chan map[string]interface{}) option {
	return func(s *Service) {
		s.msgChan = msgChan
	}
}

// WithUow устанавливает сервис для работы с хранилищами.
func WithUow(uow unitOfWork) option {
	return func(s *Service) {
		s.uow = uow
	}
}

// WithMessageRepo устанавливает репозиторий для работы с сообщениями.
func WithMessageRepo(messageRepo messageRepo) option {
	return func(s *Service) {
		s.messageRepo = messageRepo
	}
}

// WithInstanceID устанавливает id экземпляра приложения.
func WithInstanceID(instanceID int) option {
	return func(s *Service) {
		s.instanceID = instanceID
	}
}

// WithDriversMap устанавливает мапу с драйверами.
func WithDriversMap(driversMap map[string]model.Configurator) option {
	return func(s *Service) {
		s.driversMap = driversMap
	}
}

// WithMetricsService устанавливает сервис для работы с метриками.
func WithMetricsService(metricsService messageCounter) option {
	return func(s *Service) {
		s.metricsService = metricsService
	}
}

// New создает новый экземпляр сервиса.
func New(opts ...option) (*Service, error) {
	s := &Service{
		messages: make(map[uuid.UUID]*message.Message),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.cfg == nil {
		return nil, errors.New("cfg is required")
	}

	if s.msgChan == nil {
		return nil, errors.New("message channel is required")
	}

	if s.uow == nil {
		return nil, errors.New("uow is required")
	}

	if s.messageRepo == nil {
		return nil, errors.New("message repo is required")
	}

	if len(s.driversMap) == 0 {
		return nil, errors.New("drivers map is required")
	}

	if s.metricsService == nil {
		return nil, errors.New("metrics service is required")
	}

	// не проверяем instanceID, т.к. он может быть 0

	s.quitChan = make(chan struct{})

	return s, nil
}

// Run запускает сервис.
func (s *Service) Run(ctx context.Context) error {
	go s.readMessages(ctx)

	return nil
}

// Stop закрывает сервис.
func (s *Service) Stop(_ context.Context) error {
	logrus.WithFields(logrus.Fields{
		"name": s.cfg.Name,
	}).Info("operation: closing")

	close(s.quitChan)

	return nil
}
