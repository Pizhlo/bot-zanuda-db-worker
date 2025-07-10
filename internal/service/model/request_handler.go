package model

import (
	"context"
	"db-worker/internal/config"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// RequestProcessor представляет процессор запросов
type RequestProcessor struct {
	handlers map[string]RequestTypeHandler
}

// RequestTypeHandler представляет обработчик для конкретного типа запроса
type RequestTypeHandler interface {
	Process(ctx context.Context, config interface{}) error
	GetType() string
}

// NewRequestProcessor создает новый процессор запросов
func NewRequestProcessor() *RequestProcessor {
	return &RequestProcessor{
		handlers: make(map[string]RequestTypeHandler),
	}
}

// RegisterHandler регистрирует обработчик для типа запроса
func (p *RequestProcessor) RegisterHandler(handler RequestTypeHandler) {
	p.handlers[handler.GetType()] = handler
}

// ProcessRequest обрабатывает запрос согласно конфигурации
func (p *RequestProcessor) ProcessRequest(ctx context.Context, requestConfig *config.RequestConfig) error {
	handler, err := requestConfig.GetRequestHandler()
	if err != nil {
		return fmt.Errorf("error getting request handler: %w", err)
	}

	processor, exists := p.handlers[handler.GetType()]
	if !exists {
		return fmt.Errorf("no processor registered for request type: %s", handler.GetType())
	}

	// Валидируем конфигурацию
	if err := handler.Validate(); err != nil {
		return fmt.Errorf("invalid request config: %w", err)
	}

	// Обрабатываем запрос
	return processor.Process(ctx, handler)
}

// RabbitMQProcessor представляет обработчик RabbitMQ запросов
type RabbitMQProcessor struct {
	// Здесь можно добавить зависимости, например, соединение с RabbitMQ
}

// NewRabbitMQProcessor создает новый обработчик RabbitMQ
func NewRabbitMQProcessor() *RabbitMQProcessor {
	return &RabbitMQProcessor{}
}

// GetType возвращает тип обработчика
func (p *RabbitMQProcessor) GetType() string {
	return "rabbitmq"
}

// Process обрабатывает RabbitMQ запрос
func (p *RabbitMQProcessor) Process(ctx context.Context, handler interface{}) error {
	rabbitHandler, ok := handler.(config.RequestHandler)
	if !ok {
		return fmt.Errorf("invalid handler type for rabbitmq processor")
	}

	rabbitConfig, ok := rabbitHandler.(*config.RabbitMQRequest)
	if !ok {
		return fmt.Errorf("invalid rabbitmq config type")
	}

	logrus.Infof("Processing RabbitMQ request: queue=%s, routing_key=%s",
		rabbitConfig.Queue, rabbitConfig.RoutingKey)

	// Здесь будет логика обработки RabbitMQ запроса
	// Например, отправка сообщения в очередь
	time.Sleep(100 * time.Millisecond) // Имитация обработки

	logrus.Infof("Successfully processed RabbitMQ request")
	return nil
}

// HTTPProcessor представляет обработчик HTTP запросов
type HTTPProcessor struct {
	// Здесь можно добавить HTTP клиент
}

// NewHTTPProcessor создает новый обработчик HTTP
func NewHTTPProcessor() *HTTPProcessor {
	return &HTTPProcessor{}
}

// GetType возвращает тип обработчика
func (p *HTTPProcessor) GetType() string {
	return "http"
}

// Process обрабатывает HTTP запрос
func (p *HTTPProcessor) Process(ctx context.Context, handler interface{}) error {
	httpHandler, ok := handler.(config.RequestHandler)
	if !ok {
		return fmt.Errorf("invalid handler type for http processor")
	}

	httpConfig, ok := httpHandler.(*config.HTTPRequest)
	if !ok {
		return fmt.Errorf("invalid http config type")
	}

	logrus.Infof("Processing HTTP request: url=%s", httpConfig.URL)

	// Здесь будет логика обработки HTTP запроса
	// Например, отправка POST запроса
	time.Sleep(100 * time.Millisecond) // Имитация обработки

	logrus.Infof("Successfully processed HTTP request")
	return nil
}
