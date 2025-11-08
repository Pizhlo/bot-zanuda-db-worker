package operation

import (
	"context"
	"db-worker/internal/service/operation/message"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

//nolint:gocognit,funlen // цельная логика функции, много строк из-за логов
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

			s.addTotalMessages(len(ids))

			if err := s.buffer.add(ids, msg); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Error("operation: error add message to buffer")

				continue
			}

			// не начинаем обработку, если буфер не заполнен и канал не пуст
			if !s.buffer.isFull() && len(s.msgChan) > 0 {
				logrus.WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Debug("operation: buffer is not full and message channel is not empty")

				continue
			}

			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"connection": s.cfg.Request.From,
				"count":      s.buffer.count(),
				"ids":        ids,
			}).Info("operation: buffer is full or message channel is empty. Processing messages...")

			if err := s.processMessages(ctx); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Error("operation: error process messages")

				continue
			}

			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"message":    msg,
				"connection": s.cfg.Request.From,
			}).Info("operation: message processed")

			s.buffer.clear()
		}
	}
}

func (s *Service) processMessages(ctx context.Context) error {
	errs := make([]error, 0, s.buffer.count())

	for _, item := range s.buffer.getAll() {
		if err := s.processMessage(ctx, item.data, item.ids); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
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

	defer func() {
		s.deleteMessagesFromMap(ids) // удаляем сообщения из мапы, т.к. они обработаны
		s.addProcessedMessages(len(ids))
	}()

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
			s.addFailedMessages(len(ids))
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
			s.addFailedMessages(len(ids))
			return fmt.Errorf("error update messages: %w", err)
		}

		return fmt.Errorf("error build requests: %w", err)
	}

	// обновляем статус сообщений в БД: validated
	if err := s.updateMessagesStatus(ctx, message.StatusValidated, ids, nil); err != nil {
		s.addFailedMessages(len(ids))
		return fmt.Errorf("error update messages: %w", err)
	}

	err = s.uow.ExecRequests(ctx, requests, msg)
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

	s.addProcessingMessages(len(messages))

	return ids, nil
}

// updateMessagesStatus обновляет статус сообщений в БД.
// Принимает статус, список айдишников сообщений и ошибку.
// Если статус failed, то ошибка не может быть nil.
// Если статус не failed, то ошибка должна быть nil.
// Также обновляет метрики в зависимости от статуса: при обновлении статуса на failed или success - отнимает количество сообщений из количества сообщений в процессе обработки.
// При обновлении статуса на validated - добавляет количество сообщений в количество сообщений в статусе validated.
func (s *Service) updateMessagesStatus(ctx context.Context, status message.Status, ids []uuid.UUID, errMsg error) error {
	messages := make([]message.Message, 0, len(ids))

	if errMsg != nil && status != message.StatusFailed {
		return fmt.Errorf("err != nil, but status is not failed: %s", status)
	}

	if errMsg == nil && status == message.StatusFailed {
		return fmt.Errorf("err == nil, but status is failed: %s", status)
	}

	for _, id := range ids {
		msg, err := s.getMessageFromMap(id)
		if err != nil {
			return fmt.Errorf("error get message from map: %w", err)
		}

		originalStatus := msg.Status

		msg.Status = status

		if errMsg != nil {
			msg.Error = errMsg.Error()
		} else {
			msg.Error = ""
		}

		messages = append(messages, *msg)

		s.updateMetricsFromStatuses(originalStatus, status, 1)
	}

	err := s.messageRepo.UpdateMany(ctx, messages)
	if err != nil {
		return fmt.Errorf("error update messages: %w", err)
	}

	return nil
}

// updateMetricsFromStatuses обновляет метрики в зависимости от статуса:
//   - in progress -> failed: увеличивает количество сообщений в статусе failed и уменьшает количество сообщений в процессе обработки.
//   - in progress -> validated: увеличивает количество сообщений в статусе validated и уменьшает количество сообщений в процессе обработки.
//   - failed -> validated: увеличивает количество сообщений в статусе validated и уменьшает количество сообщений в статусе failed.
func (s *Service) updateMetricsFromStatuses(originalStatus, newStatus message.Status, count int) {
	if originalStatus == newStatus {
		return
	}

	if originalStatus == message.StatusInProgress && newStatus == message.StatusFailed {
		s.addFailedMessages(count)
	}

	if originalStatus == message.StatusInProgress && newStatus == message.StatusValidated {
		s.addValidatedMessages(count)
	}

	if originalStatus == message.StatusFailed && newStatus == message.StatusValidated {
		s.fromFailedToValidated(count)
	}
}

// deleteMessagesFromMap удаляет сообщения из мапы.
func (s *Service) deleteMessagesFromMap(ids []uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		delete(s.messages, id)
	}
}

// addMessageToMap добавляет сообщение в мапу.
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
