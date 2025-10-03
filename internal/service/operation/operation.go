package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"errors"

	"github.com/sirupsen/logrus"
)

// Service - сервис для выполнения операций.
type Service struct {
	cfg      *operation.Operation // конфигурация операции
	storages []storage.Driver     // драйвера для работы с хранилищами
	msgChan  chan map[string]interface{}
	quitChan chan struct{}

	mapFields map[string]operation.Field
}

// option определяет опции для сервиса.
type option func(*Service)

// WithStorages устанавливает драйвера для работы с хранилищами.
func WithStorages(storages []storage.Driver) option {
	return func(s *Service) {
		s.storages = storages
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

	if len(s.storages) == 0 {
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

	return s, nil
}

// Run запускает сервис.
func (s *Service) Run(ctx context.Context) error {
	go s.readMessages(ctx)

	return nil
}

// Stop закрывает сервис.
func (s *Service) Stop(_ context.Context) error {
	logrus.Debugf("operation %s: closing", s.cfg.Name)

	close(s.quitChan)

	return nil
}
