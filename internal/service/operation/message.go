package operation

import (
	"context"

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
			logrus.Debugf("operation %s: context done", s.cfg.Name)
			return
		case <-s.quitChan:
			logrus.Debugf("operation %s: quit channel received", s.cfg.Name)
			return
		case msg, ok := <-s.msgChan:
			if !ok {
				logrus.WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Debug("operation: message channel closed")

				return
			}

			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"message":    msg,
				"connection": s.cfg.Request.From,
			}).Debug("operation: received message")

			err := s.validateMessage(msg)
			if err != nil {
				logrus.WithError(err).Error("operation: error validate message")
				continue
			}

			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"message":    msg,
				"connection": s.cfg.Request.From,
			}).Debug("operation: message validated")
		}
	}
}
