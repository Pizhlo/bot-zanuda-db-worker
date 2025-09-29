package rabbit

import (
	"context"
	"db-worker/internal/model"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// HandleTopic - горутина для чтения канала notesTopic. Обрабатывает запросы на создание, обновление, удаление заметок.
func (s *Worker) HandleTopic(ctx context.Context) {
	logrus.Debugf("rabbit: start handle message in queue %s and routing key %s", s.queue.Name, s.config.routingKey)

	msgs, err := s.channel.Consume(
		s.queue.Name, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		logrus.Errorf("rabbit: error consume message: %+v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgs:
			body := model.Message{}

			logrus.Debugf("rabbit: new message in queue %s", s.queue.Name)

			if err := json.Unmarshal(msg.Body, &body); err != nil {
				logrus.Errorf("rabbit: error unmarshal message: %+v", err)
			}

			op, ok := body["operation"]
			if !ok {
				logrus.Errorf("rabbit: operation is required")
				continue
			}

			opStr, ok := op.(string)
			if !ok {
				logrus.Errorf("rabbit: operation is not a string")
				continue
			}

			if opStr != s.config.operation {
				logrus.Infof("rabbit: operation %s is not %s", opStr, s.config.operation)
				continue
			}

			processedFields := make(map[string]bool)

			for fieldName, field := range s.config.fields {
				if field.Required {
					processedFields[fieldName] = false
				}
			}

			for fieldName, field := range s.config.fields {
				if _, ok := body[fieldName]; !ok && field.Required {
					logrus.Errorf("rabbit: field %s not found in message", fieldName)
					continue
				}

				processedFields[fieldName] = true

				if err := field.ValidateField(body[fieldName]); err != nil {
					logrus.Errorf("rabbit: error validate field %s: %+v", fieldName, err)
					continue
				}
			}

			requestID, ok := body["request_id"]
			if !ok {
				logrus.Errorf("rabbit: request_id not found in message")
				continue
			}

			for fieldName, processed := range processedFields {
				if !processed {
					logrus.Errorf("rabbit: field %s not found in message. RequestID: %s", fieldName, requestID)
				}
			}

			s.msgChan <- body

			err = msg.Ack(false)
			if err != nil {
				logrus.Errorf("rabbit: error ack message: %+v", err)
				continue
			}

			logrus.Debugf("rabbit: successfully processed message in queue %s. RequestID: %s", s.queue.Name, requestID)
		}
	}
}
