package message

import (
	"context"
	"errors"

	interfaces "db-worker/internal/service/message/interface"

	"github.com/sirupsen/logrus"
)

// сервис для работы с сообщениями
type Service struct {
	msgChan chan interfaces.Message // канал для получения сообщений о заметках

	createHandler interfaces.Handler
}

type ServiceOption func(*Service)

func WithMsgChan(msgChan chan interfaces.Message) ServiceOption {
	return func(s *Service) {
		s.msgChan = msgChan
	}
}

func WithCreateHandler(createHandler interfaces.Handler) ServiceOption {
	return func(s *Service) {
		s.createHandler = createHandler
	}
}

func New(opts ...ServiceOption) (*Service, error) {
	s := &Service{}
	for _, opt := range opts {
		opt(s)
	}

	if s.msgChan == nil {
		return nil, errors.New("message channel is required")
	}

	if s.createHandler == nil {
		return nil, errors.New("create handler is required")
	}

	// TODO: add delete note handler

	return s, nil
}

func (s *Service) Run(ctx context.Context) {
	go func() {
		if err := s.handleMessage(ctx); err != nil {
			logrus.Errorf("message service: error handle message: %+v", err)
		}
	}()
}
