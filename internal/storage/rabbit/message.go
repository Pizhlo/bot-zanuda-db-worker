package rabbit

import (
	"context"
	"db-worker/internal/model"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// HandleNotes - горутина для чтения канала notesTopic. Обрабатывает запросы на создание, обновление, удаление заметок.
func (s *Worker) HandleNotes(ctx context.Context) {
	logrus.Debugf("rabbit: start handle message")

	msgs, err := s.channel.Consume(
		s.notesTopic.Name, // queue
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		logrus.Errorf("rabbit: error consume message: %+v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgs:
			logrus.Debugf("rabbit: received message: %s", string(msg.Body))

			var note map[string]any
			err = json.Unmarshal(msg.Body, &note)
			if err != nil {
				logrus.Errorf("rabbit: error unmarshal message: %+v", err)

				continue
			}

			operationAny, ok := note["operation"]
			if !ok {
				logrus.Errorf("rabbit: operation not found in message: %+v", note)

				continue
			}

			operation, ok := operationAny.(string)
			if !ok {
				logrus.Errorf("rabbit: operation is not a string: %+v. Request: %+v", operationAny, note["request_id"])

				continue
			}

			switch model.Operation(operation) {
			case model.CreateOp:
				var create model.CreateNoteRequest
				err = json.Unmarshal(msg.Body, &create)
				if err != nil {
					logrus.Errorf("rabbit: error unmarshal message: %+v", err)

					continue
				}

				logrus.Debugf("rabbit: received create note message: %+v", create)

				s.createNotesChan <- create
			case model.UpdateOp:
				var update model.UpdateNoteRequest
				err = json.Unmarshal(msg.Body, &update)
				if err != nil {
					logrus.Errorf("rabbit: error unmarshal message: %+v", err)

					continue
				}

				logrus.Debugf("rabbit: received update note message: %+v", update)

				s.updateNotesChan <- update
			default:
				logrus.Errorf("rabbit: unknown operation: %+v. Request: %+v", operationAny, note["request_id"])

				continue
			}
		}
	}
}
