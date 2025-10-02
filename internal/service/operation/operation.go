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

type OperationService struct {
	cfg        *operation.Operation    // конфигурация операции
	connection worker.Worker           // соединение для получения сообщений
	storages   []storage.StorageDriver // драйвера для работы с хранилищами
	msgChan    chan interfaces.Message
	quitChan   chan struct{}
}

type option func(*OperationService)

func WithStorages(storages []storage.StorageDriver) option {
	return func(s *OperationService) {
		s.storages = storages
	}
}

func WithCfg(cfg *operation.Operation) option {
	return func(s *OperationService) {
		s.cfg = cfg
	}
}

func WithConnection(connection worker.Worker) option {
	return func(s *OperationService) {
		s.connection = connection
	}
}

func WithMsgChan(msgChan chan interfaces.Message) option {
	return func(s *OperationService) {
		s.msgChan = msgChan
	}
}

func New(opts ...option) (*OperationService, error) {
	s := &OperationService{}

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

func (s *OperationService) Run(ctx context.Context) error {
	go s.readMessages(ctx)

	return nil
}

func (s *OperationService) readMessages(ctx context.Context) {
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

func (s *OperationService) Close() error {
	logrus.Debugf("operation: closing")

	close(s.quitChan)

	return nil
}
