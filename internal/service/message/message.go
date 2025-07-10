package message

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

func (s *Service) handleCreateNotes(ctx context.Context) error {
	logrus.Debugf("message service: start handle create notes")

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("message service: context done: %w", ctx.Err())
		case msg := <-s.createNotesChan:
			logrus.Debugf("message service: new create note message: %+v", msg)

			shouldSave := len(s.createNotesChan) == 0

			go func() {
				err := s.createHandler.Handle(ctx, msg, shouldSave)
				if err != nil {
					logrus.Errorf("message service: error creating note: %+v", err)
				}
			}()

		}
	}
}

func (s *Service) handleUpdateNotes(ctx context.Context) error {
	logrus.Debugf("message service: start handle update notes")

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("message service: context done: %w", ctx.Err())
		case msg := <-s.updateNotesChan:
			logrus.Debugf("message service: new update note message: %+v", msg)

			shouldSave := len(s.updateNotesChan) == 0

			go func() {
				err := s.updateHandler.Handle(ctx, msg, shouldSave)
				if err != nil {
					logrus.Errorf("message service: error updating note: %+v", err)
				}
			}()
		}
	}
}
