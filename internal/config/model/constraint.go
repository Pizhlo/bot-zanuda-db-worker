package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Constraint представляет ограничение на уровне таблицы
type Constraint struct {
	Type       string   `yaml:"type" validate:"required,oneof=unique foreign_key check"`
	Fields     []string `yaml:"fields,omitempty"`
	Field      string   `yaml:"field,omitempty"`
	References string   `yaml:"references,omitempty"`
	Check      string   `yaml:"check,omitempty"`
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

const (
	minConstraint       = "min"
	maxConstraint       = "max"
	maxLengthConstraint = "max_length"
	enumConstraint      = "enum"
	notEmptyConstraint  = "not_empty"
)

// приватный тип для хранения обработчиков валидации
type constraintValidator struct {
	handlers map[string]func(value interface{}, constraint ValidationConstraint) error
}

func newConstraintValidator() *constraintValidator {
	c := &constraintValidator{
		handlers: make(map[string]func(value interface{}, constraint ValidationConstraint) error),
	}

	c.handlers = map[string]func(value interface{}, constraint ValidationConstraint) error{
		minConstraint:       c.min,
		maxConstraint:       c.max,
		maxLengthConstraint: c.maxLength,
		enumConstraint:      c.enum,
		notEmptyConstraint:  c.notEmpty,
	}

	return c
}

func (c *constraintValidator) validate(value interface{}, constraint ValidationConstraint) error {
	f, ok := c.handlers[constraint.Type]
	if !ok {
		return fmt.Errorf("unknown constraint type: %s", constraint.Type)
	}

	return f(value, constraint)
}

// min проверяет, что значение больше или равно минимальному значению
func (c *constraintValidator) min(value interface{}, constraint ValidationConstraint) error {
	if num, ok := value.(int); ok && num < constraint.Min {
		return fmt.Errorf("value must be at least %d", constraint.Min)
	}

	if num, ok := value.(int64); ok && num < int64(constraint.Min) {
		return fmt.Errorf("value must be at least %d", constraint.Min)
	}

	return nil
}

// max проверяет, что значение меньше или равно максимальному значению
func (c *constraintValidator) max(value interface{}, constraint ValidationConstraint) error {
	if num, ok := value.(int); ok && num > constraint.Max {
		return fmt.Errorf("value must be at most %d", constraint.Max)
	}

	if num, ok := value.(int64); ok && num > int64(constraint.Max) {
		return fmt.Errorf("value must be at most %d", constraint.Max)
	}

	return nil
}

// maxLength проверяет, что длина строки меньше или равна максимальной длине
func (c *constraintValidator) maxLength(value interface{}, constraint ValidationConstraint) error {
	if str, ok := value.(string); ok && len(str) > constraint.MaxLength {
		return fmt.Errorf("string length must be at most %d", constraint.MaxLength)
	}

	return nil
}

// enum проверяет, что значение равно одному из перечисленных значений
func (c *constraintValidator) enum(value interface{}, constraint ValidationConstraint) error {
	t := reflect.TypeOf(value)

	switch t.Kind() {
	case reflect.String:
		if str, ok := value.(string); ok {
			for _, enumValue := range constraint.Enum {
				if str == enumValue {
					return nil
				}
			}

			return fmt.Errorf("value must be one of: %v. Actual: %v", constraint.Enum, value)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		for _, enumValue := range constraint.Enum {
			num, err := strconv.Atoi(enumValue)
			if err != nil {
				return fmt.Errorf("invalid enum value: %s", enumValue)
			}

			if _, ok := value.(int); ok && value.(int) == num {
				return nil
			}

			if _, ok := value.(int8); ok && value.(int8) == int8(num) {
				return nil
			}

			if _, ok := value.(int16); ok && value.(int16) == int16(num) {
				return nil
			}

			if _, ok := value.(int32); ok && value.(int32) == int32(num) {
				return nil
			}

			if _, ok := value.(int64); ok && value.(int64) == int64(num) {
				return nil
			}
		}

		return fmt.Errorf("value must be one of: %v. Actual: %v", constraint.Enum, value)

	case reflect.Float32, reflect.Float64:
		for _, enumValue := range constraint.Enum {
			num, err := strconv.ParseFloat(enumValue, 64)
			if err != nil {
				return fmt.Errorf("invalid enum value: %s", enumValue)
			}

			if _, ok := value.(float32); ok && value.(float32) == float32(num) {
				return nil
			}

			if _, ok := value.(float64); ok && value.(float64) == num {
				return nil
			}
		}

		return fmt.Errorf("value must be one of: %v. Actual: %v", constraint.Enum, value)

	case reflect.Bool:
		if value.(bool) {
			return nil
		}

		return fmt.Errorf("value must be one of: %v. Actual: %v", constraint.Enum, value)

	default:
		return fmt.Errorf("invalid enum value: %v", value)
	}

	return nil
}

// notEmpty проверяет, что значение не пустое
func (c *constraintValidator) notEmpty(value interface{}, constraint ValidationConstraint) error {
	t := reflect.TypeOf(value)

	switch t.Kind() {
	case reflect.String:
		if strings.TrimSpace(value.(string)) == "" {
			return fmt.Errorf("field cannot be empty")
		}
	case reflect.Slice, reflect.Map, reflect.Array:
		if reflect.ValueOf(value).Len() == 0 {
			return fmt.Errorf("field cannot be empty")
		}
	case reflect.Bool:
		if !value.(bool) {
			return fmt.Errorf("field cannot be empty")
		}
	}

	return nil
}
