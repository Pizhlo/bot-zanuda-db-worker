package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// ModelConfig представляет конфигурацию моделей
type ModelConfig struct {
	Models map[string]Model `yaml:"models" validate:"required"`
}

// Model представляет модель данных
type Model struct {
	Operations map[string]Operation `yaml:"operations" validate:"required"`
}

// Operation представляет операцию над моделью
type Operation struct {
	Name            string           `yaml:"name" validate:"required,oneof=create update"` // название операции
	Storage         string           `yaml:"storage" validate:"required,oneof=postgres"`   // хранилище, в котором нужно производить операцию
	Table           string           `yaml:"table" validate:"required"`                    // название таблицы, в которой будет храниться модель
	Fields          map[string]Field `yaml:"fields" validate:"required,validateFields"`    // поля, необходимые для операции
	WhereConditions map[string]Field `yaml:"where_conditions,omitempty"`                   // поля, по которым нужно производить обновление (where = ?)
	Constraints     []Constraint     `yaml:"constraints,omitempty"`                        // ограничения на уровне таблицы
	ValidationRules []ValidationRule `yaml:"validation_rules,omitempty"`                   // правила валидации на уровне операции
	Request         *RequestConfig   `yaml:"request,omitempty"`                            // конфигурация запроса
}

// Field представляет поле модели
type Field struct {
	Type       string                 `yaml:"type" validate:"required,oneof=string int int64 uuid bool"`
	Required   bool                   `yaml:"required"`
	Default    interface{}            `yaml:"default,omitempty"`
	Value      interface{}            `yaml:"value,omitempty"` // значение поля, которое будет использоваться в операции
	Validation []ValidationConstraint `yaml:"validation,omitempty"`
}

// ValidationConstraint представляет ограничение валидации
type ValidationConstraint struct {
	Type      string      `yaml:"type" validate:"required"`
	Value     interface{} `yaml:"value,omitempty"`
	Min       int         `yaml:"min,omitempty"`
	Max       int         `yaml:"max,omitempty"`
	MaxLength int         `yaml:"max_length,omitempty"`
	Enum      []string    `yaml:"enum,omitempty"`
}

// Constraint представляет ограничение на уровне таблицы
type Constraint struct {
	Type       string   `yaml:"type" validate:"required,oneof=unique foreign_key check"`
	Fields     []string `yaml:"fields,omitempty"`
	Field      string   `yaml:"field,omitempty"`
	References string   `yaml:"references,omitempty"`
	Check      string   `yaml:"check,omitempty"`
}

// ValidationRule представляет правило валидации на уровне операции
type ValidationRule struct {
	Type  string `yaml:"type" validate:"required"`
	Field string `yaml:"field,omitempty"`
}

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

// LoadModelConfig загружает конфигурацию моделей из файла
func LoadModelConfig(path string) (*ModelConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading model config: %w", err)
	}

	var config ModelConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error unmarshaling model config: %w", err)
	}

	validate := validator.New()

	err = validate.RegisterValidation("validateFields", validateFields)
	if err != nil {
		return nil, fmt.Errorf("config: error register validation: %w", err)
	}

	if err := validate.Struct(config); err != nil {
		return nil, fmt.Errorf("error validating model config: %w", err)
	}

	logrus.Infof("loaded %d model(s)", len(config.Models))

	return &config, nil
}

// ValidateField проверяет поле согласно конфигурации
func (f *Field) ValidateField(value interface{}) error {
	if f.Required && value == nil {
		return fmt.Errorf("field is required")
	}

	if value == nil {
		return nil // необязательные поля могут быть nil
	}

	// Проверяем тип
	if err := f.validateType(value); err != nil {
		return err
	}

	// Проверяем валидации
	for _, validation := range f.Validation {
		if err := f.validateConstraint(value, validation); err != nil {
			return err
		}
	}

	return nil
}

func (f *Field) validateType(value interface{}) error {
	expectedType := f.Type
	actualType := reflect.TypeOf(value).String()

	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %s", actualType)
		}
	case "int":
		if _, ok := value.(int); !ok {
			return fmt.Errorf("expected int, got %s", actualType)
		}
	case "int64":
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("expected int64, got %s", actualType)
		}
	case "uuid":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected uuid string, got %s", actualType)
		}

		err := uuid.Validate(value.(string))
		if err != nil {
			return fmt.Errorf("invalid uuid: %w", err)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %s", actualType)
		}
	}

	return nil
}

func (f *Field) validateConstraint(value interface{}, constraint ValidationConstraint) error {
	switch constraint.Type {
	case "not_empty":
		if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
			return fmt.Errorf("field cannot be empty")
		}
	case "min":
		if num, ok := value.(int); ok && num < constraint.Min {
			return fmt.Errorf("value must be at least %d", constraint.Min)
		}
		if num, ok := value.(int64); ok && num < int64(constraint.Min) {
			return fmt.Errorf("value must be at least %d", constraint.Min)
		}
	case "max":
		if num, ok := value.(int); ok && num > constraint.Max {
			return fmt.Errorf("value must be at most %d", constraint.Max)
		}
		if num, ok := value.(int64); ok && num > int64(constraint.Max) {
			return fmt.Errorf("value must be at most %d", constraint.Max)
		}
	case "max_length":
		if str, ok := value.(string); ok && len(str) > constraint.MaxLength {
			return fmt.Errorf("string length must be at most %d", constraint.MaxLength)
		}
	case "enum":
		if str, ok := value.(string); ok {
			found := false
			for _, enumValue := range constraint.Enum {
				if str == enumValue {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("value must be one of: %v", constraint.Enum)
			}
		}
	}

	return nil
}

// GetFieldValue возвращает значение поля с учетом дефолтного значения
func (f *Field) GetFieldValue(value interface{}) interface{} {
	if value != nil {
		return value
	}
	return f.Default
}

// BuildWhereClause строит WHERE условие для операции
func (o *Operation) BuildWhereClause(conditions map[string]interface{}) (string, []interface{}, error) {
	if len(o.WhereConditions) == 0 {
		return "", nil, nil
	}

	var clauses []string
	var values []interface{}

	for fieldName, field := range o.WhereConditions {
		value, exists := conditions[fieldName]
		if !exists && field.Required {
			return "", nil, fmt.Errorf("required where condition field missing: %s", fieldName)
		}

		if exists {
			if err := field.ValidateField(value); err != nil {
				return "", nil, fmt.Errorf("invalid where condition field %s: %w", fieldName, err)
			}

			clauses = append(clauses, fmt.Sprintf("%s = ?", fieldName))
			values = append(values, value)
		}
	}

	return strings.Join(clauses, " AND "), values, nil
}

func validateFields(fl validator.FieldLevel) bool {
	fields := fl.Field()

	// Проверяем, что fields является map[string]Field
	if fields.Kind() != reflect.Map {
		return false
	}

	// Получаем значение поля operation
	operationField := fields.MapIndex(reflect.ValueOf("operation"))
	if !operationField.IsValid() {
		return false
	}

	// Проверяем, что operation является Field
	if operationField.Kind() != reflect.Struct {
		return false
	}

	// Получаем значение поля Value
	valueField := operationField.FieldByName("Value")
	if !valueField.IsValid() {
		return false
	}

	// Проверяем значение
	value := valueField.Interface()
	if str, ok := value.(string); ok {
		return str == "create" || str == "update"
	}

	return false
}

// GetRequestHandler возвращает обработчик запроса на основе типа
func (r *RequestConfig) GetRequestHandler() (RequestHandler, error) {
	switch r.From {
	case "rabbitmq":
		var config RabbitMQRequest
		if err := r.unmarshalConfig(&config); err != nil {
			return nil, fmt.Errorf("invalid rabbitmq config: %w", err)
		}
		return &config, nil
	case "http":
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

// GetType возвращает тип RabbitMQ запроса
func (r *RabbitMQRequest) GetType() string {
	return "rabbitmq"
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
	return nil
}

// GetType возвращает тип HTTP запроса
func (r *HTTPRequest) GetType() string {
	return "http"
}

// Validate валидирует HTTP конфигурацию
func (r *HTTPRequest) Validate() error {
	if r.URL == "" {
		return fmt.Errorf("url is required for http request")
	}
	return nil
}
