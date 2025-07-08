package rabbit

import (
	"context"
	"db-worker/internal/model"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// HandleNotes - горутина для чтения канала notesTopic. Обрабатывает запросы на создание заметок.
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

			var note model.CreateNoteRequest
			err = json.Unmarshal(msg.Body, &note)
			if err != nil {
				logrus.Errorf("rabbit: error unmarshal message: %+v", err)

				continue
			}

			s.msgChan <- note
		}
	}
}
