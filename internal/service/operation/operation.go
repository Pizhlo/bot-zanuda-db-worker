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
	cfg *operation.Operation // конфигурация операции

	uow unitOfWork

	msgChan  chan map[string]interface{}
	quitChan chan struct{}
}

// unitOfWork инкапсулирует логику работы с хранилищами. Берет на себя построение запросов и выполнение их.
//
//go:generate mockgen -source=operation.go -destination=mocks/unit_of_work_mock.go -package=operation unitOfWork
type unitOfWork interface {
	BuildRequests(msg map[string]interface{}) (map[storage.Driver]*storage.Request, error)
	ExecRequests(ctx context.Context, requests map[storage.Driver]*storage.Request) error
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

// New создает новый экземпляр сервиса.
func New(opts ...option) (*Service, error) {
	s := &Service{}

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
