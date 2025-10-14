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

	case FieldTypeBool:
		return validateExpectedValueBool(f)

	default:
		return fmt.Errorf("unsupported field type %q", f.Type)
	}
}

func validateExpectedValueString(f Field) error {
	s, ok := f.Validation.ExpectedValue.(string)
	if !ok {
		return fmt.Errorf("field %s: expected value is not string", f.Name)
	}

	if f.Validation.MinLength != nil && len(s) < int(*f.Validation.MinLength) {
		return fmt.Errorf("field %s: expected value shorter than min_length", f.Name)
	}

	if f.Validation.MaxLength != nil && len(s) > int(*f.Validation.MaxLength) {
		return fmt.Errorf("field %s: expected value longer than max_length", f.Name)
	}

	return nil
}

func validateExpectedValueInt64(f Field) error {
	i, ok := f.Validation.ExpectedValue.(int)
	if !ok {
		return fmt.Errorf("field %s: expected value is not int64", f.Name)
	}

	if f.Validation.Min != nil && i < *f.Validation.Min {
		return fmt.Errorf("field %s: expected value is less than min", f.Name)
	}

	if f.Validation.Max != nil && i > *f.Validation.Max {
		return fmt.Errorf("field %s: expected value is greater than max", f.Name)
	}

	return nil
}

func validateExpectedValueFloat64(f Field) error {
	fVal, ok := f.Validation.ExpectedValue.(float64)
	if !ok {
		return fmt.Errorf("field %s: expected value is not float64", f.Name)
	}

	if f.Validation.Min != nil && fVal < float64(*f.Validation.Min) {
		return fmt.Errorf("field %s: expected value is less than min", f.Name)
	}

	if f.Validation.Max != nil && fVal > float64(*f.Validation.Max) {
		return fmt.Errorf("field %s: expected value is greater than max", f.Name)
	}

	return nil
}

func validateExpectedValueUUID(f Field) error {
	s, ok := f.Validation.ExpectedValue.(string)
	if !ok {
		return fmt.Errorf("field %s: expected value is not string", f.Name)
	}

	if _, err := uuid.Parse(s); err != nil {
		return fmt.Errorf("field %s: expected value is not valid uuid", f.Name)
	}

	return nil
}

func validateExpectedValueBool(f Field) error {
	_, ok := f.Validation.ExpectedValue.(bool)
	if !ok {
		return fmt.Errorf("field %s: expected value is not bool", f.Name)
	}

	// дальше нет валидации, т.к. если удалось привести к bool, то это валидное значение

	return nil
}

// validateWhereConditions валидирует согласованность количества полей и наличия оператора в условиях where.
//   - если в условии одно поле, operator type должен отсутствовать (пустая строка)
//   - если полей больше одного, operator type обязателен
//   - если заполнено value в where, то в сообщении должно быть такое же значение
//   - where недопустимо для операций create
//   - where опционален для операций update, delete и delete_all
//
// WARNING: запускать после того, как отработал метод mapFieldsByOperation.
func (op *Operation) validateWhereCondition() error {
	if op.Type == OperationTypeCreate && len(op.Where) > 0 {
		return fmt.Errorf("where condition: operation %q: where is not allowed for create operation", op.Name)
	}

	for i, w := range op.Where {
		if err := validateWhereCondition(w, op.FieldsMap, op.Name, i); err != nil {
			return err
		}
	}

	return nil
}

func validateWhereCondition(w Where, fieldsMap map[string]Field, opName string, idx int) error {
	// когда объединены несколько условий
	if len(w.Conditions) > 0 {
		return validateMultipleWhereCondition(w, fieldsMap, opName, idx)
	}

	// одно условие
	return validateSingleWhereCondition(w, fieldsMap, opName, idx)
}

// для случая, когда одно условие.
func validateSingleWhereCondition(w Where, fieldsMap map[string]Field, opName string, idx int) error {
	fieldsCount := len(w.Fields)

	if fieldsCount == 0 {
		return fmt.Errorf("where condition %d: operation %q: fields must not be empty", idx, opName)
	}

	// если поле одно, то type должен отсутствовать (where user_id = 10)
	if fieldsCount == 1 && w.Type != "" {
		return fmt.Errorf("where condition %d: operation %q: type is not empty, but fields count is 1", idx, opName)
	}

	// если полей больше одного, то type должен присутствовать (where user_id = 10 and name = "test")
	if fieldsCount > 1 && w.Type == "" {
		return fmt.Errorf("where condition %d: operation %q: type is empty, but fields count is > 1", idx, opName)
	}

	for _, whereField := range w.Fields {
		if err := validateWhereField(whereField, fieldsMap, opName, idx); err != nil {
			return err
		}
	}

	return nil
}

func validateWhereField(whereField WhereField, fieldsMap map[string]Field, opName string, idx int) error {
	// достаем обычное поле из сообщения
	field, ok := fieldsMap[whereField.Name]
	if !ok {
		return fmt.Errorf("where condition %d: operation %q: field %q is not found", idx, opName, whereField.Name)
	}

	// проверяем, что value и expected_value совпадают (если заполнены)
	if whereField.Value != nil {
		if field.Validation.ExpectedValue == nil {
			return fmt.Errorf("where condition %d: operation %q: expected value is not set for field %q, but set in where condition", idx, opName, whereField.Name)
		}

		return validateFieldValues(field, whereField, opName, idx)
	}

	return nil
}

// для случая, когда объединены несколько условий.
func validateMultipleWhereCondition(w Where, fieldsMap map[string]Field, opName string, idx int) error {
	if w.Type == "" {
		return fmt.Errorf("where condition %d: operation %q: type is empty", idx, opName)
	}

	// комбинированный узел не должен содержать собственных полей
	if len(w.Fields) > 0 {
		return fmt.Errorf("where condition %d: operation %q: fields must be empty when conditions are combined", idx, opName)
	}

	for _, condition := range w.Conditions {
		if err := validateWhereCondition(condition, fieldsMap, opName, idx); err != nil {
			return err
		}
	}

	return nil
}

func validateFieldValues(field Field, whereField WhereField, opName string, idx int) error {
	switch field.Type {
	case FieldTypeString, FieldTypeUUID:
		if err := compareValues[string](whereField.Value, field.Validation.ExpectedValue); err != nil {
			return fmt.Errorf("where condition %d: operation %q: %w", idx, opName, err)
		}

	case FieldTypeInt64:
		if err := compareValues[int64](whereField.Value, field.Validation.ExpectedValue); err != nil {
			return fmt.Errorf("where condition %d: operation %q: %w", idx, opName, err)
		}

	case FieldTypeFloat64:
		if err := compareValues[float64](whereField.Value, field.Validation.ExpectedValue); err != nil {
			return fmt.Errorf("where condition %d: operation %q: %w", idx, opName, err)
		}

	case FieldTypeBool:
		if err := compareValues[bool](whereField.Value, field.Validation.ExpectedValue); err != nil {
			return fmt.Errorf("where condition %d: operation %q: %w", idx, opName, err)
		}
	}

	return nil
}

func compareValues[K comparable](value any, expectedVal any) error {
	val, ok1 := value.(K)
	if !ok1 {
		return fmt.Errorf("value must be of type %T", val)
	}

	expectedVal, ok2 := expectedVal.(K)
	if !ok2 || val != expectedVal {
		return fmt.Errorf("expected value must be of type %T and be equal to %v", expectedVal, val)
	}

	return nil
}
