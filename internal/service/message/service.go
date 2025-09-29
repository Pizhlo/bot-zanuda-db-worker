package message

import (
	"context"
	"errors"

	interfaces "db-worker/internal/service/message/interface"

	"github.com/sirupsen/logrus"
)

// сервис для работы с сообщениями
type Service struct {
	createChannels []chan interfaces.Message
	updateChannels []chan interfaces.Message

	createHandler interfaces.Handler
	updateHandler interfaces.Handler
}

type ServiceOption func(*Service)

func WithCreateChannels(createChannels []chan interfaces.Message) ServiceOption {
	return func(s *Service) {
		s.createChannels = createChannels
	}
}

func WithUpdateChannels(updateChannels []chan interfaces.Message) ServiceOption {
	return func(s *Service) {
		s.updateChannels = updateChannels
	}
}

func WithCreateHandler(createHandler interfaces.Handler) ServiceOption {
	return func(s *Service) {
		s.createHandler = createHandler
	}
}

func WithUpdateHandler(updateHandler interfaces.Handler) ServiceOption {
	return func(s *Service) {
		s.updateHandler = updateHandler
	}
}

func New(opts ...ServiceOption) (*Service, error) {
	s := &Service{}
	for _, opt := range opts {
		opt(s)
	}

	if s.createChannels == nil {
		return nil, errors.New("create channels is required")
	}

	if s.updateChannels == nil {
		return nil, errors.New("update channels is required")
	}

	if s.createHandler == nil {
		return nil, errors.New("create handler is required")
	}

	if s.updateHandler == nil {
		return nil, errors.New("update handler is required")
	}

	// TODO: add delete note handler

	return s, nil
}

func (s *Service) Run(ctx context.Context) {
	for _, ch := range s.createChannels {
		go func() {
			if err := s.handleCreateOperation(ctx, ch); err != nil {
				logrus.Errorf("message service: error handle create operation: %+v", err)
			}
		}()
	}

	for _, ch := range s.updateChannels {
		go func() {
			if err := s.handleUpdateOperation(ctx, ch); err != nil {
				logrus.Errorf("message service: error handle update operation: %+v", err)
			}
		}()
	}
}
