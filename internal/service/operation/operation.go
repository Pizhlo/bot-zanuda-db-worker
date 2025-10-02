package operation

import (
	"context"
	"db-worker/internal/config/operation"
	interfaces "db-worker/internal/service/message/interface"
	"db-worker/internal/service/worker"
	"db-worker/internal/storage"
	"errors"

	"github.com/sirupsen/logrus"
)

// Service - сервис для выполнения операций.
type Service struct {
	cfg        *operation.Operation // конфигурация операции
	connection worker.Worker        // соединение для получения сообщений
	storages   []storage.Driver     // драйвера для работы с хранилищами
	msgChan    chan interfaces.Message
	quitChan   chan struct{}
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

// WithConnection устанавливает соединение для получения сообщений.
func WithConnection(connection worker.Worker) option {
	return func(s *Service) {
		s.connection = connection
	}
}

// WithMsgChan устанавливает канал для получения сообщений.
func WithMsgChan(msgChan chan interfaces.Message) option {
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

	if s.connection == nil {
		return nil, errors.New("connection is required")
	}

	if len(s.storages) == 0 {
		return nil, errors.New("storages are required")
	}

	if s.msgChan == nil {
		return nil, errors.New("message channel is required")
	}

	s.quitChan = make(chan struct{})

	return s, nil
}

// Run запускает сервис.
func (s *Service) Run(ctx context.Context) error {
	go s.readMessages(ctx)

	return nil
}

func (s *Service) readMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logrus.Debugf("operation: context done")
			return
		case <-s.quitChan:
			logrus.Debugf("operation: quit channel received")
			return
		case msg := <-s.msgChan:
			logrus.Debugf("operation: received message: %+v", msg)
		}
	}
}

// Close закрывает сервис.
func (s *Service) Close() error {
	logrus.Debugf("operation: closing")

	close(s.quitChan)

	return nil
}
