package rabbit

import (
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Worker struct {
	config struct {
		address string

		// queues
		notesTopic  string
		spacesTopic string
	}

	msgChan chan interfaces.Message

	// queues
	notesTopic  amqp.Queue
	spacesTopic amqp.Queue

	conn    *amqp.Connection
	channel *amqp.Channel

	insertTimeout int
	readTimeout   int
}

type RabbitOption func(*Worker)

func WithAddress(address string) RabbitOption {
	return func(w *Worker) {
		w.config.address = address
	}
}

func WithMsgChan(msgCh chan interfaces.Message) RabbitOption {
	return func(w *Worker) {
		w.msgChan = msgCh
	}
}

func WithNotesTopic(notesTopic string) RabbitOption {
	return func(w *Worker) {
		w.config.notesTopic = notesTopic
	}
}

func WithSpacesTopic(spacesTopic string) RabbitOption {
	return func(w *Worker) {
		w.config.spacesTopic = spacesTopic
	}
}

func WithInsertTimeout(insertTimeout int) RabbitOption {
	return func(w *Worker) {
		w.insertTimeout = insertTimeout
	}
}

func WithReadTimeout(readTimeout int) RabbitOption {
	return func(w *Worker) {
		w.readTimeout = readTimeout
	}
}

func New(opts ...RabbitOption) (*Worker, error) {
	w := &Worker{}

	for _, opt := range opts {
		opt(w)
	}

	if w.insertTimeout == 0 {
		return nil, fmt.Errorf("insert timeout is required")
	}

	if w.readTimeout == 0 {
		return nil, fmt.Errorf("read timeout is required")
	}

	if w.config.address == "" {
		return nil, fmt.Errorf("rabbit: address is required")
	}

	if w.config.notesTopic == "" {
		return nil, fmt.Errorf("rabbit: notes topic is required")
	}

	if w.config.spacesTopic == "" {
		return nil, fmt.Errorf("rabbit: spaces topic is required")
	}

	if w.msgChan == nil {
		return nil, fmt.Errorf("rabbit: notes message channel is required")
	}

	return w, nil
}

func (s *Worker) Connect() error {
	conn, err := amqp.Dial(s.config.address)
	if err != nil {
		return fmt.Errorf("rabbit: error creating connection: %w", err)
	}

	s.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbit: error creating connection: %w", err)
	}

	s.channel = ch

	notesTopic, err := ch.QueueDeclare(
		s.config.notesTopic, // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return fmt.Errorf("error creating queue %s: %+v", s.config.notesTopic, err)
	}

	s.notesTopic = notesTopic

	spacesTopic, err := ch.QueueDeclare(
		s.config.spacesTopic, // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return fmt.Errorf("error creating queue %s: %+v", s.config.spacesTopic, err)
	}

	s.spacesTopic = spacesTopic

	return nil
}

func (s *Worker) Close() error {
	err := s.channel.Close()
	if err != nil {
		logrus.Errorf("worker: error closing channel rabbit mq: %+v", err)
	}

	if err := s.conn.Close(); err != nil {
		logrus.Errorf("worker: error closing connection rabbit mq: %+v", err)
	}

	return nil
}
