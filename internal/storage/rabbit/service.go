package rabbit

import (
	config "db-worker/internal/config/model"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Worker struct {
	config struct {
		address    string
		topic      string
		routingKey string
		fields     map[string]config.Field
		operation  string
	}

	msgChan chan interfaces.Message

	queue amqp.Queue

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

func WithMsgChan(msgChan chan interfaces.Message) RabbitOption {
	return func(w *Worker) {
		w.msgChan = msgChan
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

func WithTopic(topic string) RabbitOption {
	return func(w *Worker) {
		w.config.topic = topic
	}
}

func WithFields(fields map[string]config.Field) RabbitOption {
	return func(w *Worker) {
		w.config.fields = fields
	}
}

func WithOperation(operation string) RabbitOption {
	return func(w *Worker) {
		w.config.operation = operation
	}
}

func WithRoutingKey(routingKey string) RabbitOption {
	return func(w *Worker) {
		w.config.routingKey = routingKey
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

	if w.msgChan == nil {
		return nil, fmt.Errorf("rabbit: message channel is required")
	}

	if w.config.fields == nil {
		return nil, fmt.Errorf("rabbit: model is required")
	}

	if w.config.operation == "" {
		return nil, fmt.Errorf("rabbit: operation is required")
	}

	if w.config.routingKey == "" {
		return nil, fmt.Errorf("rabbit: routing key is required")
	}

	return w, nil
}

func (s *Worker) Connect(topic string) error {
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

	// Declare topic exchange for notes
	err = ch.ExchangeDeclare(
		"notes", // name
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("error declaring exchange notes: %+v", err)
	}

	// Create unique queue name for this worker instance
	queueName := fmt.Sprintf("%s_%s_%s", topic, s.config.operation, s.config.routingKey)

	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("error creating queue %s: %+v", queueName, err)
	}

	// Bind queue to exchange with specific routing key
	err = ch.QueueBind(
		queue.Name,
		s.config.routingKey, // routing key (create, update, delete)
		"notes",             // exchange name
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error binding queue %s to routing key %s: %+v", queue.Name, s.config.routingKey, err)
	}

	s.queue = queue
	s.config.topic = topic

	logrus.Infof("successfully created queue %s bound to exchange 'notes' with routing key '%s'", queueName, s.config.routingKey)

	return nil
}

func (s *Worker) Close() {
	err := s.channel.Close()
	if err != nil {
		logrus.Errorf("worker: error closing channel rabbit mq: %+v", err)
	}

	if err := s.conn.Close(); err != nil {
		logrus.Errorf("worker: error closing connection rabbit mq: %+v", err)
	}
}
