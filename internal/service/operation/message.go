package operation

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

func (s *Service) readMessages(ctx context.Context) {
	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"connection": s.cfg.Request.From,
	}).Info("operation: start read messages")

	for {
		select {
		case <-ctx.Done():
			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"connection": s.cfg.Request.From,
			}).Info("operation: context done")

			return
		case <-s.quitChan:
			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"connection": s.cfg.Request.From,
			}).Info("operation: quit channel received")

			return
		case msg, ok := <-s.msgChan:
			if !ok {
				logrus.WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Info("operation: message channel closed")

				return
			}

			if err := s.processMessage(ctx, msg); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Error("operation: error process message")

				continue
			}

			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"message":    msg,
				"connection": s.cfg.Request.From,
			}).Info("operation: message processed")
		}
	}
}

// processMessage обрабатывает сообщение - валидирует, строит запросы и передает на выполнение в UOW.
func (s *Service) processMessage(ctx context.Context, msg map[string]interface{}) error {
	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"message":    msg,
		"connection": s.cfg.Request.From,
	}).Info("operation: received message")

	err := s.validateMessage(msg)
	if err != nil {
		return fmt.Errorf("error validate message: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"message":    msg,
		"connection": s.cfg.Request.From,
	}).Info("operation: message validated")

	requests, err := s.uow.BuildRequests(msg)
	if err != nil {
		return fmt.Errorf("error build requests: %w", err)
	}

	err = s.uow.ExecRequests(ctx, requests)
	if err != nil {
		return fmt.Errorf("error exec requests: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"name":           s.cfg.Name,
		"message":        msg,
		"connection":     s.cfg.Request.From,
		"requests_count": len(requests),
	}).Info("operation: requests executed")

	return nil
}
