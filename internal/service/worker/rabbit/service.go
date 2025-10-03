package rabbit

import (
	"context"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Worker соединение с RabbitMQ.
type Worker struct {
	config struct {
		address string
		name    string

		exchange   string
		routingKey string
	}

	msgChan  chan interfaces.Message
	quitChan chan struct{}

	msgs <-chan amqp.Delivery

	// queues
	queue amqp.Queue

	conn    *amqp.Connection
	channel *amqp.Channel

	insertTimeout int
	readTimeout   int
}

// Option определяет опции для Worker.
type Option func(*Worker)

// WithName устанавливает имя соединения.
func WithName(name string) Option {
	return func(w *Worker) {
		w.config.name = name
	}
}

// WithAddress устанавливает адрес соединения.
func WithAddress(address string) Option {
	return func(w *Worker) {
		w.config.address = address
	}
}

// WithExchange устанавливает exchange.
func WithExchange(exchange string) Option {
	return func(w *Worker) {
		w.config.exchange = exchange
	}
}

// WithRoutingKey устанавливает routing key.
func WithRoutingKey(routingKey string) Option {
	return func(w *Worker) {
		w.config.routingKey = routingKey
	}
}

// WithInsertTimeout устанавливает время ожидания вставки.
func WithInsertTimeout(insertTimeout int) Option {
	return func(w *Worker) {
		w.insertTimeout = insertTimeout
	}
}

// WithReadTimeout устанавливает время ожидания чтения.
func WithReadTimeout(readTimeout int) Option {
	return func(w *Worker) {
		w.readTimeout = readTimeout
	}
}

// New создает новый экземпляр Worker.
func New(opts ...Option) (*Worker, error) {
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

// Connect соединяется с RabbitMQ.
func (s *Worker) Connect() error {
	err := s.connectQueue()
	if err != nil {
		return fmt.Errorf("rabbit: error connecting queue: %w", err)
	}

	err = s.connectChannel()
	if err != nil {
		return fmt.Errorf("rabbit: error connecting channel: %w", err)
	}

	return nil
}

func (s *Worker) connectQueue() error {
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

	logrus.WithFields(logrus.Fields{
		"address":        s.config.address,
		"name":           s.config.name,
		"exchange":       s.config.exchange,
		"routing_key":    s.config.routingKey,
		"insert_timeout": s.insertTimeout,
		"read_timeout":   s.readTimeout,
	}).Info("successfully connected rabbit")

	return nil
}

// Name возвращает имя соединения.
func (s *Worker) Name() string {
	return s.config.name
}

// MsgChan возвращает канал для получения сообщений.
func (s *Worker) MsgChan() chan interfaces.Message {
	return s.msgChan
}

// Stop закрывает соединение с RabbitMQ.
func (s *Worker) Stop(_ context.Context) error {
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
