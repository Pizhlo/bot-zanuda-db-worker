package validator

import (
	"db-worker/internal/config/operation"
	"fmt"

	"github.com/google/uuid"
)

type validatorFunc func(field operation.Field, val any) error

//nolint:gochecknoglobals // используется только в этом модуле.
var validatorsMap = map[operation.FieldType]validatorFunc{
	operation.FieldTypeInt64:   validateInt64,
	operation.FieldTypeFloat64: validateFloat64,
	operation.FieldTypeBool:    validateBool,
	operation.FieldTypeUUID:    validateUUID,
	operation.FieldTypeString:  validateString,
}

type validator struct {
	field operation.Field
	val   any
	// в будущем будут добавлены ограничения на поля
}

// New создает новый экземпляр валидатора.
func New() *validator {
	return &validator{}
}

// WithField устанавливает поле для валидации.
func (v *validator) WithField(field operation.Field) *validator {
	v.field = field
	return v
}

// WithVal устанавливает значение для валидации.
func (v *validator) WithVal(val any) *validator {
	v.val = val
	return v
}

// Validate валидирует значение.
func (v *validator) Validate() error {
	if v.field.Type == "" {
		return fmt.Errorf("field type is required")
	}

	if v.val == nil {
		return fmt.Errorf("value is required")
	}

	validate, err := forField(v.field)
	if err != nil {
		return err
	}

	return validate(v.field, v.val)
}

// forField возвращает валидатор для поля в зависимости от типа поля.
func forField(field operation.Field) (validatorFunc, error) {
	validator, ok := validatorsMap[field.Type]
	if !ok {
		return nil, fmt.Errorf("validator not found for field type: %s", field.Type)
	}

	return validator, nil
}

func validateInt64(field operation.Field, val any) error {
	v, ok := val.(int64)
	if !ok {
		return validateFloat64(field, val) // иногда int64 приходит как float64
	}

	return validateInt64Value(field, v)
}

func validateFloat64(field operation.Field, val any) error {
	v, ok := val.(float64)
	if !ok {
		return fmt.Errorf("field %q is not a float64", field.Name)
	}

	return validateFloat64Value(field, v)
}

func validateBool(field operation.Field, val any) error {
	v, ok := val.(bool)
	if !ok {
		return fmt.Errorf("field %q is not a bool", field.Name)
	}

	return validateBoolValue(field, v)
}

func validateUUID(field operation.Field, val any) error {
	v, ok := val.(uuid.UUID)
	if !ok {
		return fmt.Errorf("field %q is not a uuid", field.Name)
	}

	_, err := uuid.Parse(v.String())
	if err != nil {
		return fmt.Errorf("field %q must be a valid uuid", field.Name)
	}

	// здесь нет валидации, т.к. uuid.Parse покрывает все случаи

	return nil
}

func validateString(field operation.Field, val any) error {
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf("field %q is not a string", field.Name)
	}

	return validateStringValue(field, v)
}
