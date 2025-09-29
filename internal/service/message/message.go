package message

import (
	"context"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
)

func (s *Service) handleCreateOperation(ctx context.Context, ch chan interfaces.Message) error {
	logrus.Debugf("message service: start handle create operation")

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("message service: context done: %w", ctx.Err())
		case msg := <-ch:
			logrus.Debugf("message service: new create operation: %+v", msg)

			shouldSave := len(ch) == 0

			go func() {
				err := s.createHandler.Handle(ctx, msg, shouldSave)
				if err != nil {
					logrus.Errorf("message service: error processing create operation: %+v", err)
				}
			}()

		}
	}
}

func (s *Service) handleUpdateOperation(ctx context.Context, ch chan interfaces.Message) error {
	logrus.Debugf("message service: start handle update operation")

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("message service: context done: %w", ctx.Err())
		case msg := <-ch:
			logrus.Debugf("message service: new update operation: %+v", msg)

			shouldSave := len(ch) == 0

			go func() {
				err := s.updateHandler.Handle(ctx, msg, shouldSave)
				if err != nil {
					logrus.Errorf("message service: error processing update operation: %+v", err)
				}
			}()
		}
	}
}
