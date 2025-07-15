package config

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

// Field представляет поле модели
type Field struct {
	Type       string                 `yaml:"type" validate:"required,oneof=string int int64 uuid bool"`
	Required   bool                   `yaml:"required" validate:"required,bool"`
	Default    interface{}            `yaml:"default,omitempty"`
	Value      interface{}            `yaml:"value,omitempty"` // значение поля, которое будет использоваться в операции
	Validation []ValidationConstraint `yaml:"validation,omitempty"`
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

	field := newFieldValidator()
	if err := field.validateType(value, expectedType); err != nil {
		return fmt.Errorf("invalid type: %w", err)
	}

	return nil
}

func (f *Field) validateConstraint(value interface{}, constraint ValidationConstraint) error {
	c := newConstraintValidator()
	if err := c.validate(value, constraint); err != nil {
		return fmt.Errorf("error validate constraint: %w", err)
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

type fieldValidator struct {
	handlers map[string]func(value interface{}) error
}

func newFieldValidator() *fieldValidator {
	f := &fieldValidator{
		handlers: make(map[string]func(value interface{}) error),
	}

	f.handlers[fieldTypeString] = f.validateString
	f.handlers[fieldTypeInt] = f.validateInt
	f.handlers[fieldTypeInt64] = f.validateInt64
	f.handlers[fieldTypeUUID] = f.validateUUID
	f.handlers[fieldTypeBool] = f.validateBool

	return f
}

const (
	fieldTypeString = "string"
	fieldTypeInt    = "int"
	fieldTypeInt64  = "int64"
	fieldTypeUUID   = "uuid"
	fieldTypeBool   = "bool"
)

func (f *fieldValidator) validateType(value interface{}, expectedType string) error {
	handler, ok := f.handlers[expectedType]
	if !ok {
		return fmt.Errorf("invalid type: %s", expectedType)
	}

	return handler(value)
}

func (f *fieldValidator) validateString(value interface{}) error {
	if _, ok := value.(string); !ok {
		return fmt.Errorf("expected string, got %s", reflect.TypeOf(value).String())
	}

	return nil
}

func (f *fieldValidator) validateInt(value interface{}) error {
	if _, ok := value.(int); !ok {
		return fmt.Errorf("expected int, got %s", reflect.TypeOf(value).String())
	}

	return nil
}

func (f *fieldValidator) validateInt64(value interface{}) error {
	if _, ok := value.(int64); !ok {
		switch v := value.(type) {
		case float64:
			if v != float64(int(v)) { // если нет дробной части - ок
				return fmt.Errorf("expected int64, got %s", reflect.TypeOf(value).String())
			}

			return nil
		default:
			return fmt.Errorf("expected int64, got %s", reflect.TypeOf(value).String())
		}
	}

	return nil
}

func (f *fieldValidator) validateUUID(value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected uuid string, got %s", reflect.TypeOf(value).String())
	}

	err := uuid.Validate(v)
	if err != nil {
		return fmt.Errorf("invalid uuid: %w", err)
	}

	return nil
}

func (f *fieldValidator) validateBool(value interface{}) error {
	if _, ok := value.(bool); !ok {
		return fmt.Errorf("expected bool, got %s", reflect.TypeOf(value).String())
	}

	return nil
}
