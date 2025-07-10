package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

// RequestConfig представляет конфигурацию запроса
type RequestConfig struct {
	From   string      `yaml:"from" validate:"required,oneof=rabbitmq http"`
	Config interface{} `yaml:"config" validate:"required"`
}

// RabbitMQRequest представляет конфигурацию RabbitMQ запроса
type RabbitMQRequest struct {
	Queue      string                 `yaml:"queue" validate:"required"`
	Address    string                 `yaml:"address" validate:"required,rabbitmq_address"`
	RoutingKey string                 `yaml:"routing_key" validate:"required"`
	Message    map[string]interface{} `yaml:"message" validate:"required"`
}

// HTTPRequest представляет конфигурацию HTTP запроса
type HTTPRequest struct {
	URL  string                 `yaml:"url" validate:"required,url"`
	Body map[string]interface{} `yaml:"body,omitempty"`
}

// RequestHandler представляет интерфейс для обработки запросов
type RequestHandler interface {
	GetType() string
	Validate() error
}

func (r *RequestConfig) Validate() error {
	if r.From == RabbitMQRequestType {
		var v RabbitMQRequest
		if err := r.unmarshalConfig(&v); err != nil {
			return fmt.Errorf("invalid rabbitmq config: %w", err)
		}

		return v.Validate()
	}

	if r.From == HTTPRequestType {
		var v HTTPRequest
		if err := r.unmarshalConfig(&v); err != nil {
			return fmt.Errorf("invalid http config: %w", err)
		}

		return v.Validate()
	}

	return fmt.Errorf("invalid request type: %s", r.From)
}

// GetRequestHandler возвращает обработчик запроса на основе типа
func (r *RequestConfig) GetRequestHandler() (RequestHandler, error) {
	switch r.From {
	case RabbitMQRequestType:
		var config RabbitMQRequest
		if err := r.unmarshalConfig(&config); err != nil {
			return nil, fmt.Errorf("invalid rabbitmq config: %w", err)
		}
		return &config, nil
	case HTTPRequestType:
		var config HTTPRequest
		if err := r.unmarshalConfig(&config); err != nil {
			return nil, fmt.Errorf("invalid http config: %w", err)
		}
		return &config, nil
	default:
		return nil, fmt.Errorf("unsupported request type: %s", r.From)
	}
}

// unmarshalConfig преобразует interface{} в конкретную структуру
func (r *RequestConfig) unmarshalConfig(target interface{}) error {
	data, err := yaml.Marshal(r.Config)
	if err != nil {
		return fmt.Errorf("error marshaling config: %w", err)
	}

	return yaml.Unmarshal(data, target)
}

const (
	RabbitMQRequestType = "rabbitmq"
	HTTPRequestType     = "http"
	OperationTypeCreate = "create"
	OperationTypeUpdate = "update"
)

// GetType возвращает тип RabbitMQ запроса
func (r *RabbitMQRequest) GetType() string {
	return RabbitMQRequestType
}

// Validate валидирует RabbitMQ конфигурацию
func (r *RabbitMQRequest) Validate() error {
	if r.Queue == "" {
		return fmt.Errorf("queue is required for rabbitmq request")
	}

	if r.RoutingKey == "" {
		return fmt.Errorf("routing_key is required for rabbitmq request")
	}

	if len(r.Message) == 0 {
		return fmt.Errorf("message is required for rabbitmq request")
	}

	v, ok := r.Message["operation"]
	if !ok {
		return fmt.Errorf("message must contain operation field")
	}

	operationMap, ok := v.(map[any]any)
	if !ok {
		return fmt.Errorf("operation must be a map[any]any")
	}

	typeField, ok := operationMap["type"].(string)
	if !ok {
		return fmt.Errorf("operation must contain `type` field")
	}

	if typeField != fieldTypeString {
		return fmt.Errorf("operation type must be a string")
	}

	required, ok := operationMap["required"].(bool)
	if !ok {
		return fmt.Errorf("operation must contain `required` field")
	}

	if !required {
		return fmt.Errorf("operation must be required")
	}

	value, ok := operationMap["value"].(string)
	if !ok {
		return fmt.Errorf("operation must contain `value` field")
	}

	if value != OperationTypeCreate && value != OperationTypeUpdate {
		return fmt.Errorf("operation must be create or update")
	}

	return nil
}

// GetType возвращает тип HTTP запроса
func (r *HTTPRequest) GetType() string {
	return HTTPRequestType
}

// Validate валидирует HTTP конфигурацию
func (r *HTTPRequest) Validate() error {
	if r.URL == "" {
		return fmt.Errorf("url is required for http request")
	}

	// пока что нет валидации для HTTP запроса

	return nil
}
