package rabbit

import (
	"context"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

func (s *Worker) connectChannel() error {
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

	s.msgs = msgs

	return nil
}

// Run запускает чтение сообщений из очереди.
func (s *Worker) Run(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"routing_key": s.config.routingKey,
		"queue":       s.queue.Name,
	}).Info("rabbit: start consume messages")

	for {
		select {
		case <-ctx.Done():
			logrus.WithFields(logrus.Fields{
				"routing_key": s.config.routingKey,
				"queue":       s.queue.Name,
			}).Info("rabbit: ctx done: stop consume messages")

			return nil

		case <-s.quitChan:
			logrus.WithFields(logrus.Fields{
				"routing_key": s.config.routingKey,
				"queue":       s.queue.Name,
			}).Info("rabbit: quit chan: stop consume messages")

			return nil

		case msg := <-s.msgs:
			logrus.WithFields(logrus.Fields{
				"message":     string(msg.Body),
				"routing_key": msg.RoutingKey,
				"queue":       s.queue.Name,
			}).Debug("rabbit: received message")

			var mapMsg map[string]interface{}

			err := json.Unmarshal(msg.Body, &mapMsg)
			if err != nil {
				logrus.WithError(err).Error("rabbit: error marshal message")
				continue
			}

			go func() {
				s.msgChan <- mapMsg

				logrus.WithFields(logrus.Fields{
					"message":     string(msg.Body),
					"routing_key": msg.RoutingKey,
					"queue":       s.queue.Name,
				}).Debug("rabbit: sent message to channel")
			}()
		}
	}
}
