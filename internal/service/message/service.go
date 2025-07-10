package message

import (
	"context"
	"errors"

	interfaces "db-worker/internal/service/message/interface"

	"github.com/sirupsen/logrus"
)

// сервис для работы с сообщениями
type Service struct {
	createNotesChan chan interfaces.Message
	updateNotesChan chan interfaces.Message

	createHandler interfaces.Handler
	updateHandler interfaces.Handler
}

type ServiceOption func(*Service)

func WithCreateNotesChan(createNotesChan chan interfaces.Message) ServiceOption {
	return func(s *Service) {
		s.createNotesChan = createNotesChan
	}
}

func WithUpdateNotesChan(updateNotesChan chan interfaces.Message) ServiceOption {
	return func(s *Service) {
		s.updateNotesChan = updateNotesChan
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

	if s.createNotesChan == nil {
		return nil, errors.New("create notes channel is required")
	}

	if s.updateNotesChan == nil {
		return nil, errors.New("update notes channel is required")
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
	go func() {
		if err := s.handleCreateNotes(ctx); err != nil {
			logrus.Errorf("message service: error handle create notes: %+v", err)
		}
	}()

	go func() {
		if err := s.handleUpdateNotes(ctx); err != nil {
			logrus.Errorf("message service: error handle update notes: %+v", err)
		}
	}()
}
