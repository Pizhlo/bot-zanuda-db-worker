package rabbit

import (
	"context"
	interfaces "db-worker/internal/service/message/interface"
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Run - горутина для чтения очереди.
func (s *Worker) Run(ctx context.Context) error {
	msgs, err := s.channel.Consume(
		s.queue.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return err
	}

	logrus.Infof("rabbit: start consume messages from %s", s.queue.Name)

	go s.readMessages(ctx, msgs)

	return nil
}

func (s *Worker) readMessages(ctx context.Context, msgs <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			logrus.Infof("rabbit: stop consume messages from %s", s.queue.Name)
			return
		case <-s.quitChan:
			logrus.Infof("rabbit: stop consume messages from %s", s.queue.Name)
			return
		case msg := <-msgs:
			logrus.Debugf("rabbit: received message: %s", string(msg.Body))

			var mapMsg map[string]interface{}

			err := json.Unmarshal(msg.Body, &mapMsg)
			if err != nil {
				logrus.Errorf("rabbit: error marshal message: %+v", err)
				continue
			}

			s.msgChan <- interfaces.Message(mapMsg)

			logrus.Debugf("rabbit: sent message to channel: %+v", mapMsg)
		}
	}
}
