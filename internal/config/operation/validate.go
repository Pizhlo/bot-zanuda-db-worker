package operation

import (
	"fmt"

	"github.com/google/uuid"
)

// Validation - валидация поля.
type Validation struct {
	// Тип валидации. Например, max, min, max_length, min_length.
	Type ValidationType `yaml:"type" validate:"required,oneof=not_empty max_length min_length max min expected_value"`
	// Ожидаемое значение, если хотим принимать только 1 значение из сообщений. Например, задать, чтобы получать только user_id=1234.
	Value interface{} `yaml:"value,omitempty"` // не у всех валидаций есть значение (например, not_empty)
}

// ValidationType - тип валидации.
type ValidationType string

const (
	// ValidationTypeNotEmpty - не пустое значение.
	ValidationTypeNotEmpty ValidationType = "not_empty"
	// ValidationTypeMaxLength - максимальная длина.
	ValidationTypeMaxLength ValidationType = "max_length"
	// ValidationTypeMinLength - минимальная длина.
	ValidationTypeMinLength ValidationType = "min_length"
	// ValidationTypeMax - максимальное значение.
	ValidationTypeMax ValidationType = "max"
	// ValidationTypeMin - минимальное значение.
	ValidationTypeMin ValidationType = "min"
	// ValidationTypeExpectedValue - ожидаемое значение.
	ValidationTypeExpectedValue ValidationType = "expected_value"
)

// Rule - правило валидации.
type Rule string

const (
	// RuleMin - минимальное значение.
	RuleMin Rule = "min"
	// RuleMax - максимальное значение.
	RuleMax Rule = "max"
	// RuleMinLength - минимальная длина.
	RuleMinLength Rule = "min_length"
	// RuleMaxLength - максимальная длина.
	RuleMaxLength Rule = "max_length"
	// RuleNotEmpty - не пустое значение.
	RuleNotEmpty Rule = "not_empty"
	// RuleValue - ожидаемое значение.
	RuleValue Rule = "value"
)

// мапа с разрешенными валидациями для каждого типа.
//
//   - строки: min_length, max_length, not_empty, value
//   - int64: min, max, value
//   - uuid: value
//   - float64: min, max, value
//   - bool: value
//
//nolint:gochecknoglobals // глобальная мапа для избежания switch-case, приватная и используется только в этом модуле.
var allowedByType = map[FieldType]map[Rule]bool{
	FieldTypeString: {
		RuleMin:       false,
		RuleMax:       false,
		RuleMinLength: true,
		RuleMaxLength: true,
		RuleNotEmpty:  true,
		RuleValue:     true,
	},
	FieldTypeInt64: {
		RuleMin:       true,
		RuleMax:       true,
		RuleMinLength: false,
		RuleMaxLength: false,
		RuleNotEmpty:  false,
		RuleValue:     true,
	},
	FieldTypeUUID: {
		RuleMin:       false,
		RuleMax:       false,
		RuleMinLength: false,
		RuleMaxLength: false,
		RuleNotEmpty:  true,
		RuleValue:     true,
	},
	FieldTypeFloat64: {
		RuleMin:       true,
		RuleMax:       true,
		RuleMinLength: false,
		RuleMaxLength: false,
		RuleNotEmpty:  false,
		RuleValue:     true,
	},
	FieldTypeBool: {
		RuleMin:       false,
		RuleMax:       false,
		RuleMinLength: false,
		RuleMaxLength: false,
		RuleNotEmpty:  false,
		RuleValue:     true,
	},
}

func validateFieldConfig(f Field) error {
	if err := validateRuleCompatibility(f); err != nil {
		return err
	}

	if err := validateBoundaryConsistency(f); err != nil {
		return err
	}

	if err := validateExpectedValueConsistency(f); err != nil {
		return err
	}

	return nil
}

// validateRuleCompatibility валидирует совместимость правил валидации с типом поля.
//   - у string не может быть max, min.
//   - у int64 не может быть max_length, min_length, not_empty.
//   - у float64 не может быть max, min, not_empty.
//   - у uuid не может быть max, min.
//   - у bool не может быть max, min, max_length, min_length, not_empty.
func validateRuleCompatibility(f Field) error {
	allowed := allowedByType[f.Type]
	check := func(rule Rule, enabled bool) error {
		if enabled && !allowed[rule] {
			return fmt.Errorf("field %s: rule %q not allowed for type %q", f.Name, rule, f.Type)
		}

		return nil
	}

	if err := check(RuleMin, f.Validation.Min != nil); err != nil {
		return err
	}

	if err := check(RuleMax, f.Validation.Max != nil); err != nil {
		return err
	}

	if err := check(RuleMinLength, f.Validation.MinLength != nil); err != nil {
		return err
	}

	if err := check(RuleMaxLength, f.Validation.MaxLength != nil); err != nil {
		return err
	}

	if err := check(RuleNotEmpty, f.Validation.NotEmpty); err != nil {
		return err
	}

	if err := check(RuleValue, f.Validation.ExpectedValue != nil); err != nil {
		return err
	}

	return nil
}

// validateBoundaryConsistency валидирует соответствие границ ограничений.
func validateBoundaryConsistency(f Field) error {
	if f.Type == FieldTypeInt64 || f.Type == FieldTypeFloat64 {
		if f.Validation.Min != nil && f.Validation.Max != nil && *f.Validation.Max < *f.Validation.Min {
			return fmt.Errorf("field %s: max must be > min", f.Name)
		}
	}

	if f.Type == FieldTypeString {
		if f.Validation.MinLength != nil && f.Validation.MaxLength != nil && *f.Validation.MaxLength < *f.Validation.MinLength {
			return fmt.Errorf("field %s: max_length must be > min_length", f.Name)
		}
	}

	return nil
}

// validateExpectedValueConsistency валидирует соответствие ожидаемого значения типу поля и его ограничениям.
func validateExpectedValueConsistency(f Field) error {
	if f.Validation.ExpectedValue == nil {
		return nil
	}

	switch f.Type {
	case FieldTypeString:
		return validateExpectedValueString(f)

	case FieldTypeInt64:
		return validateExpectedValueInt64(f)

	case FieldTypeFloat64:
		return validateExpectedValueFloat64(f)

	case FieldTypeUUID:
		return validateExpectedValueUUID(f)

	default:
		return fmt.Errorf("unsupported field type %q", f.Type)
	}
}

func validateExpectedValueString(f Field) error {
	s, ok := f.Validation.ExpectedValue.(string)
	if !ok {
		return fmt.Errorf("field %s: value is not string", f.Name)
	}

	if f.Validation.MinLength != nil && len(s) < int(*f.Validation.MinLength) {
		return fmt.Errorf("field %s: value shorter than min_length", f.Name)
	}

	if f.Validation.MaxLength != nil && len(s) > int(*f.Validation.MaxLength) {
		return fmt.Errorf("field %s: value longer than max_length", f.Name)
	}

	return nil
}

func validateExpectedValueInt64(f Field) error {
	i, ok := f.Validation.ExpectedValue.(int)
	if !ok {
		return fmt.Errorf("field %s: value is not int64", f.Name)
	}

	if f.Validation.Min != nil && i < *f.Validation.Min {
		return fmt.Errorf("field %s: value is less than min", f.Name)
	}

	if f.Validation.Max != nil && i > *f.Validation.Max {
		return fmt.Errorf("field %s: value is greater than max", f.Name)
	}

	return nil
}

func validateExpectedValueFloat64(f Field) error {
	fVal, ok := f.Validation.ExpectedValue.(float64)
	if !ok {
		return fmt.Errorf("field %s: value is not float64", f.Name)
	}

	if f.Validation.Min != nil && fVal < float64(*f.Validation.Min) {
		return fmt.Errorf("field %s: value is less than min", f.Name)
	}

	if f.Validation.Max != nil && fVal > float64(*f.Validation.Max) {
		return fmt.Errorf("field %s: value is greater than max", f.Name)
	}

	return nil
}

func validateExpectedValueUUID(f Field) error {
	s, ok := f.Validation.ExpectedValue.(string)
	if !ok {
		return fmt.Errorf("field %s: value is not string", f.Name)
	}

	if _, err := uuid.Parse(s); err != nil {
		return fmt.Errorf("field %s: value is not valid uuid", f.Name)
	}

	return nil
}
