package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

// Service - сервис для выполнения операций.
type Service struct {
	cfg         *operation.Operation      // конфигурация операции
	storagesMap map[string]storage.Driver // драйвера для работы с хранилищами
	driversMap  map[string]drivers        // поле для сопоставления драйвера хранения и конфигурации
	msgChan     chan map[string]interface{}
	quitChan    chan struct{}

	mapFields map[string]operation.Field
}

type drivers struct {
	driver storage.Driver
	cfg    operation.Storage
}

// option определяет опции для сервиса.
type option func(*Service)

// WithStorages устанавливает драйвера для работы с хранилищами.
func WithStorages(storages []storage.Driver) option {
	return func(s *Service) {
		s.storagesMap = make(map[string]storage.Driver)

		for _, storage := range storages {
			s.storagesMap[string(storage.Name())] = storage
		}
	}
}

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

// New создает новый экземпляр сервиса.
func New(opts ...option) (*Service, error) {
	s := &Service{}

	for _, opt := range opts {
		opt(s)
	}

	if s.cfg == nil {
		return nil, errors.New("cfg is required")
	}

	if len(s.storagesMap) == 0 {
		return nil, errors.New("storages are required")
	}

	if s.msgChan == nil {
		return nil, errors.New("message channel is required")
	}

	s.quitChan = make(chan struct{})
	s.mapFields = make(map[string]operation.Field)

	for _, field := range s.cfg.Fields {
		s.mapFields[field.Name] = field
	}

	if err := s.mapStorages(); err != nil {
		return nil, fmt.Errorf("error mapping storages: %w", err)
	}

	return s, nil
}

func (s *Service) mapStorages() error {
	s.driversMap = make(map[string]drivers)

	for _, storage := range s.cfg.Storages {
		driver, ok := s.storagesMap[string(storage.Name)]
		if !ok {
			return fmt.Errorf("storage %q not found", storage.Name)
		}

		s.driversMap[storage.Name] = drivers{
			driver: driver,
			cfg:    storage,
		}
	}

	return nil
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
	}).Debug("operation: closing")

	close(s.quitChan)

	return nil
}
