package message

import (
	"context"
	"fmt"

	"db-worker/internal/model"

	"github.com/sirupsen/logrus"
)

func (s *Service) handleMessage(ctx context.Context) error {
	logrus.Debugf("message service: start handle message")

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("message service: context done: %w", ctx.Err())
		case msg := <-s.msgChan:
			logrus.Debugf("message service: new message: %+v", msg)

			shouldSave := len(s.msgChan) == 0

			switch msg.MessageType() {
			case model.MessageTypeNoteCreate:
				go func() {
					err := s.createHandler.Handle(ctx, msg, shouldSave)
					if err != nil {
						logrus.Errorf("message service: error creating note: %+v", err)
					}
				}()
			}

		}
	}
}
