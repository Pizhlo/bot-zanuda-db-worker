package operation

import (
	"context"
	"db-worker/internal/service/operation/message"
	"fmt"

	"github.com/google/uuid"
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

			// сохраняем в БД сообщения для каждого драйвера
			ids, err := s.createMessages(ctx, msg)
			if err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Error("operation: error create messages")

				s.deleteMessagesFromMap(ids) // удаляем сообщения из карты, т.к. они не были созданы

				continue
			}

			if err := s.processMessage(ctx, msg, ids); err != nil {
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
// Принимает сообщение и список айдишников созданных сообщений.
//
//nolint:funlen // цельная логика функции, много строк из-за логов
func (s *Service) processMessage(ctx context.Context, msg map[string]any, ids []uuid.UUID) error {
	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"message":    msg,
		"connection": s.cfg.Request.From,
		"ids":        ids,
	}).Info("operation: received message")

	defer s.deleteMessagesFromMap(ids) // удаляем сообщения из мапы, т.к. они обработаны

	err := s.validateMessage(msg)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"name":       s.cfg.Name,
			"message":    msg,
			"connection": s.cfg.Request.From,
			"ids":        ids,
		}).Error("operation: error validate message")

		// обновляем статус сообщений в БД: failed
		if err := s.updateMessagesStatus(ctx, message.StatusFailed, ids, err); err != nil {
			return fmt.Errorf("error update messages: %w", err)
		}

		return fmt.Errorf("error validate message: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"message":    msg,
		"connection": s.cfg.Request.From,
		"ids":        ids,
	}).Info("operation: message validated")

	requests, err := s.uow.BuildRequests(msg, s.uow.StoragesMap(), *s.cfg)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"name":       s.cfg.Name,
			"message":    msg,
			"connection": s.cfg.Request.From,
			"ids":        ids,
		}).Error("operation: error build requests")

		// обновляем статус сообщений в БД: failed
		if err := s.updateMessagesStatus(ctx, message.StatusFailed, ids, err); err != nil {
			return fmt.Errorf("error update messages: %w", err)
		}

		return fmt.Errorf("error build requests: %w", err)
	}

	// обновляем статус сообщений в БД: validated
	if err := s.updateMessagesStatus(ctx, message.StatusValidated, ids, nil); err != nil {
		return fmt.Errorf("error update messages: %w", err)
	}

	err = s.uow.ExecRequests(ctx, requests)
	if err != nil {
		// не обновляем статус сообщений, т.к. валидация прошла успешно, а дальше это работа UOW
		return fmt.Errorf("error exec requests: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"name":           s.cfg.Name,
		"message":        msg,
		"connection":     s.cfg.Request.From,
		"requests_count": len(requests),
		"ids":            ids,
	}).Info("operation: requests executed")

	return nil
}

// createMessages создает экземпляры сообщений для каждого драйвера и возвращает список айдишников созданных сообщений.
func (s *Service) createMessages(ctx context.Context, msg map[string]any) ([]uuid.UUID, error) {
	messages := make([]message.Message, 0, len(s.driversMap))

	ids := make([]uuid.UUID, 0, len(s.driversMap))

	for _, driver := range s.driversMap {
		msg := &message.Message{
			ID:            uuid.New(),
			Data:          msg,
			Status:        message.StatusInProgress,
			DriverType:    string(driver.Type()),
			DriverName:    driver.Name(),
			InstanceID:    s.instanceID,
			OperationHash: s.cfg.Hash,
		}

		messages = append(messages, *msg)
		s.addMessageToMap(msg.ID, msg)
		ids = append(ids, msg.ID)
	}

	err := s.messageRepo.CreateMany(ctx, messages)
	if err != nil {
		return ids, fmt.Errorf("error create messages: %w", err)
	}

	return ids, nil
}

// updateMessagesStatus обновляет статус сообщений в БД.
// Принимает статус, список айдишников сообщений и ошибку.
// Если статус failed, то ошибка не может быть nil.
// Если статус не failed, то ошибка должна быть nil.
func (s *Service) updateMessagesStatus(ctx context.Context, status message.Status, ids []uuid.UUID, errMsg error) error {
	messages := make([]message.Message, 0, len(ids))

	if errMsg != nil && status != message.StatusFailed {
		return fmt.Errorf("err != nil, but status is not failed: %s", status)
	}

	if errMsg == nil && status == message.StatusFailed {
		return fmt.Errorf("err == nil, but status is failed: %s", status)
	}

	for _, id := range ids {
		message, err := s.getMessageFromMap(id)
		if err != nil {
			return fmt.Errorf("error get message from map: %w", err)
		}

		message.Status = status

		if errMsg != nil {
			message.Error = errMsg.Error()
		} else {
			message.Error = ""
		}

		messages = append(messages, *message)
	}

	err := s.messageRepo.UpdateMany(ctx, messages)
	if err != nil {
		return fmt.Errorf("error update messages: %w", err)
	}

	return nil
}

func (s *Service) deleteMessagesFromMap(ids []uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		delete(s.messages, id)
	}
}

func (s *Service) addMessageToMap(id uuid.UUID, message *message.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[id] = message
}

func (s *Service) getMessageFromMap(id uuid.UUID) (*message.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	message, ok := s.messages[id]
	if !ok {
		return nil, fmt.Errorf("message not found by id: %s", id)
	}

	return message, nil
}
