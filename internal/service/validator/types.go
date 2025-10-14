package validator

import (
	"db-worker/internal/config/operation"
	"fmt"
)

func validateInt64Value(field operation.Field, val int64) error {
	validation := field.Validation

	if validation.ExpectedValue != nil {
		expectedValue, _ := validation.ExpectedValue.(int64)
		if expectedValue != val {
			return fmt.Errorf("field %q must be %d, but got %d", field.Name, expectedValue, val)
		}
	}

	// не проверяем на not_empty, т.к. 0 - тоже валидное значение
	// не проверяем на max_length, min_length, т.к. это недопустимо для int64

	if field.Validation.Max != nil {
		if val > int64(*field.Validation.Max) {
			return fmt.Errorf("field %q must be less than %d, but got %d", field.Name, *field.Validation.Max, val)
		}
	}

	if field.Validation.Min != nil {
		if val < int64(*field.Validation.Min) {
			return fmt.Errorf("field %q must be greater than %d, but got %d", field.Name, *field.Validation.Min, val)
		}
	}

	return nil
}

func validateFloat64Value(field operation.Field, val float64) error {
	validation := field.Validation

	if validation.ExpectedValue != nil {
		expectedValue, ok := validation.ExpectedValue.(float64)
		if !ok {
			expValueInt, ok := validation.ExpectedValue.(int)
			if !ok {
				return fmt.Errorf("field %q: invalid type of expected value", field.Name)
			}

			expectedValue = float64(expValueInt)
		}

		if expectedValue != val {
			return fmt.Errorf("field %q must be %f, but got %f", field.Name, expectedValue, val)
		}
	}

	// не проверяем на not_empty, т.к. 0 - тоже валидное значение
	// не проверяем на max_length, min_length, т.к. это недопустимо для float64

	if field.Validation.Max != nil {
		if val > float64(*field.Validation.Max) {
			return fmt.Errorf("field %q must be less than %v, but got %f", field.Name, *field.Validation.Max, val)
		}
	}

	if field.Validation.Min != nil {
		if val < float64(*field.Validation.Min) {
			return fmt.Errorf("field %q must be greater than %v, but got %f", field.Name, *field.Validation.Min, val)
		}
	}

	return nil
}

func validateStringValue(field operation.Field, val string) error {
	len := len(val)

	validation := field.Validation

	if validation.ExpectedValue != nil {
		expectedValue, _ := validation.ExpectedValue.(string)
		if expectedValue != val {
			return fmt.Errorf("field %q must be %s, but got %s", field.Name, expectedValue, val)
		}
	}

	if field.Validation.NotEmpty {
		if len == 0 {
			return fmt.Errorf("field %q must be not empty", field.Name)
		}
	}

	if field.Validation.MaxLength != nil {
		if len > int(*field.Validation.MaxLength) {
			return fmt.Errorf("length of field %q must be less than %d, but got %d", field.Name, *field.Validation.MaxLength, len)
		}
	}

	if field.Validation.MinLength != nil {
		if len < int(*field.Validation.MinLength) {
			return fmt.Errorf("length of field %q must be greater than %d, but got %d", field.Name, *field.Validation.MinLength, len)
		}
	}

	return nil
}

func validateBoolValue(field operation.Field, val bool) error {
	if field.Validation.ExpectedValue != nil {
		expectedValue, _ := field.Validation.ExpectedValue.(bool)
		if expectedValue != val {
			return fmt.Errorf("field %q must be %t, but got %t", field.Name, expectedValue, val)
		}
	}

	return nil
}
