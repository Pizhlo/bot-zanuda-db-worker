package model

import (
	"context"
	"db-worker/internal/config"
	"fmt"
	"reflect"

	"github.com/sirupsen/logrus"
)

// Service представляет универсальный сервис для работы с моделями
type Service struct {
	modelConfig      *config.ModelConfig
	handlers         map[string]Handler
	requestProcessor *RequestProcessor
	messageChan      chan map[string]interface{}
}

// Handler представляет обработчик для конкретной модели
type Handler interface {
	Create(ctx context.Context, data map[string]interface{}) error
	Update(ctx context.Context, data map[string]interface{}, conditions map[string]interface{}) error
	Delete(ctx context.Context, conditions map[string]interface{}) error
	Get(ctx context.Context, conditions map[string]interface{}) (map[string]interface{}, error)
}

// ServiceOption представляет опцию для конфигурации сервиса
type ServiceOption func(*Service)

// WithModelConfig устанавливает конфигурацию моделей
func WithModelConfig(cfg *config.ModelConfig) ServiceOption {
	return func(s *Service) {
		s.modelConfig = cfg
	}
}

// WithHandler регистрирует обработчик для модели
func WithHandler(modelName string, handler Handler) ServiceOption {
	return func(s *Service) {
		if s.handlers == nil {
			s.handlers = make(map[string]Handler)
		}
		s.handlers[modelName] = handler
	}
}

// WithRequestProcessor устанавливает процессор запросов
func WithRequestProcessor(processor *RequestProcessor) ServiceOption {
	return func(s *Service) {
		s.requestProcessor = processor
	}
}

// WithMessageChan устанавливает канал для отправки сообщений
func WithMessageChan(messageChan chan map[string]interface{}) ServiceOption {
	return func(s *Service) {
		s.messageChan = messageChan
	}
}

// New создает новый экземпляр сервиса
func New(opts ...ServiceOption) (*Service, error) {
	s := &Service{
		handlers: make(map[string]Handler),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.modelConfig == nil {
		return nil, fmt.Errorf("model config is required")
	}

	if s.messageChan == nil {
		return nil, fmt.Errorf("message chan is required")
	}

	return s, nil
}

func (s *Service) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-s.messageChan:
			logrus.Debugf("model service: received message: %+v", message)

			go func() {
				err := s.ProcessOperation(ctx, message)
				if err != nil {
					logrus.Errorf("model service: error processing message: %+v", err)
				}
			}()
		}
	}
}

// ProcessOperation обрабатывает операцию над моделью
func (s *Service) ProcessOperation(ctx context.Context, data map[string]interface{}) error {
	logrus.Debugf("model service: processing message: %+v", data)

	operationName, ok := data["operation"].(string)
	if !ok {
		return fmt.Errorf("operation is not a string or not specified")
	}

	modelName, ok := data["model_name"].(string)
	if !ok {
		return fmt.Errorf("model is not a string or not specified")
	}

	model, exists := s.modelConfig.Models[modelName]
	if !exists {
		return fmt.Errorf("model %s not found in config", modelName)
	}

	operation, exists := model.Operations[operationName]
	if !exists {
		return fmt.Errorf("operation %s not found for model %s", operationName, modelName)
	}

	// Валидируем данные согласно конфигурации
	if err := s.validateOperationData(&operation, data); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Обрабатываем запрос, если он настроен
	if operation.Request != nil && s.requestProcessor != nil {
		if err := s.requestProcessor.ProcessRequest(ctx, operation.Request); err != nil {
			return fmt.Errorf("request processing failed: %w", err)
		}
	}

	// Получаем обработчик для модели
	handler, exists := s.handlers[modelName]
	if !exists {
		return fmt.Errorf("handler not found for model %s", modelName)
	}

	// Выполняем операцию
	switch operationName {
	case "create":
		return handler.Create(ctx, data)
	case "update":
		// Для update нужны условия WHERE
		whereConditions := make(map[string]interface{})
		for fieldName := range operation.WhereConditions {
			if value, exists := data[fieldName]; exists {
				whereConditions[fieldName] = value
			}
		}
		return handler.Update(ctx, data, whereConditions)
	case "delete":
		whereConditions := make(map[string]interface{})
		for fieldName := range operation.WhereConditions {
			if value, exists := data[fieldName]; exists {
				whereConditions[fieldName] = value
			}
		}
		return handler.Delete(ctx, whereConditions)
	default:
		return fmt.Errorf("unsupported operation: %s", operationName)
	}
}

// validateOperationData валидирует данные согласно конфигурации операции
func (s *Service) validateOperationData(operation *config.Operation, data map[string]interface{}) error {
	// Проверяем обязательные поля
	for fieldName, field := range operation.Fields {
		value, exists := data[fieldName]

		if field.Required && !exists {
			return fmt.Errorf("required field %s is missing", fieldName)
		}

		if exists {
			// Валидируем поле
			if err := field.ValidateField(value); err != nil {
				return fmt.Errorf("field %s validation failed: %w", fieldName, err)
			}
		} else if field.Default != nil {
			// Устанавливаем дефолтное значение
			data[fieldName] = field.Default
		}
	}

	// Проверяем фиксированные значения
	for fieldName, field := range operation.Fields {
		if field.Value != nil {
			value, exists := data[fieldName]
			if !exists {
				data[fieldName] = field.Value
			} else if !reflect.DeepEqual(value, field.Value) {
				return fmt.Errorf("field %s must have value %v, got %v", fieldName, field.Value, value)
			}
		}
	}

	return nil
}

// GetModelInfo возвращает информацию о модели
func (s *Service) GetModelInfo(modelName string) (*config.Model, error) {
	model, exists := s.modelConfig.Models[modelName]
	if !exists {
		return nil, fmt.Errorf("model %s not found", modelName)
	}
	return &model, nil
}

// ListModels возвращает список доступных моделей
func (s *Service) ListModels() []string {
	models := make([]string, 0, len(s.modelConfig.Models))
	for modelName := range s.modelConfig.Models {
		models = append(models, modelName)
	}
	return models
}

// ListOperations возвращает список операций для модели
func (s *Service) ListOperations(modelName string) ([]string, error) {
	model, exists := s.modelConfig.Models[modelName]
	if !exists {
		return nil, fmt.Errorf("model %s not found", modelName)
	}

	operations := make([]string, 0, len(model.Operations))
	for operationName := range model.Operations {
		operations = append(operations, operationName)
	}
	return operations, nil
}

// ValidateData валидирует данные для конкретной модели и операции
func (s *Service) ValidateData(modelName, operationName string, data map[string]interface{}) error {
	model, exists := s.modelConfig.Models[modelName]
	if !exists {
		return fmt.Errorf("model %s not found", modelName)
	}

	operation, exists := model.Operations[operationName]
	if !exists {
		return fmt.Errorf("operation %s not found for model %s", operationName, modelName)
	}

	return s.validateOperationData(&operation, data)
}
