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
		name    string

		exchange   string
		routingKey string
	}

	msgChan  chan interfaces.Message
	quitChan chan struct{}

	// queues
	queue amqp.Queue

	conn    *amqp.Connection
	channel *amqp.Channel

	insertTimeout int
	readTimeout   int
}

type RabbitOption func(*Worker)

func WithName(name string) RabbitOption {
	return func(w *Worker) {
		w.config.name = name
	}
}

func WithAddress(address string) RabbitOption {
	return func(w *Worker) {
		w.config.address = address
	}
}

func WithExchange(exchange string) RabbitOption {
	return func(w *Worker) {
		w.config.exchange = exchange
	}
}

func WithRoutingKey(routingKey string) RabbitOption {
	return func(w *Worker) {
		w.config.routingKey = routingKey
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
		return nil, fmt.Errorf("rabbit: insert timeout is required")
	}

	if w.readTimeout == 0 {
		return nil, fmt.Errorf("rabbit: read timeout is required")
	}

	if w.config.name == "" {
		return nil, fmt.Errorf("rabbit: name is required")
	}

	if w.config.address == "" {
		return nil, fmt.Errorf("rabbit: address is required")
	}

	if w.config.exchange == "" {
		return nil, fmt.Errorf("rabbit: exchange is required")
	}

	if w.config.routingKey == "" {
		return nil, fmt.Errorf("rabbit: routing key is required")
	}

	w.msgChan = make(chan interfaces.Message)
	w.quitChan = make(chan struct{})

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
		return fmt.Errorf("rabbit: error creating channel: %w", err)
	}

	s.channel = ch

	// Объявляем exchange
	err = ch.ExchangeDeclare(
		s.config.exchange, // name
		"topic",           // type
		true,              // durable
		false,             // auto-delete
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("error creating exchange %s: %w", s.config.exchange, err)
	}

	// Создаем queue
	s.queue, err = ch.QueueDeclare(
		"notes_queue", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return fmt.Errorf("error creating queue: %w", err)
	}

	// Привязываем queue к exchange с routing key
	err = ch.QueueBind(
		s.queue.Name,        // queue name
		s.config.routingKey, // routing key
		s.config.exchange,   // exchange
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return fmt.Errorf("error binding queue to exchange: %w", err)
	}

	logrus.Infof("successfully connected rabbit on %s", s.config.address)

	return nil
}

func (s *Worker) Name() string {
	return s.config.name
}

func (s *Worker) MsgChan() chan interfaces.Message {
	return s.msgChan
}

func (s *Worker) Close() error {
	err := s.channel.Close()
	if err != nil {
		logrus.Errorf("worker: error closing channel rabbit mq: %+v", err)
	}

	if err := s.conn.Close(); err != nil {
		logrus.Errorf("worker: error closing connection rabbit mq: %+v", err)
	}

	close(s.quitChan)

	return nil
}
