package validator

import (
	"db-worker/internal/config/operation"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	v := New()

	assert.NotNil(t, v)
	assert.Empty(t, v.field)
	assert.Nil(t, v.val)
}

func TestWithField(t *testing.T) {
	t.Parallel()

	v := New()
	field := operation.Field{
		Name: "test_field",
		Type: operation.FieldTypeString,
	}

	result := v.WithField(field)

	// Проверяем, что возвращается тот же экземпляр (fluent API)
	assert.Equal(t, v, result)
	assert.Equal(t, field, v.field)
}

func TestWithVal(t *testing.T) {
	t.Parallel()

	v := New()
	val := "test_value"

	result := v.WithVal(val)

	// Проверяем, что возвращается тот же экземпляр (fluent API)
	assert.Equal(t, v, result)
	assert.Equal(t, val, v.val)
}

//nolint:funlen // тестовая функция
func TestValidate(t *testing.T) {
	t.Parallel()

	testUUID := uuid.New()

	testCases := []struct {
		name        string
		field       operation.Field
		value       any
		expectError bool
		errorMsg    string
	}{
		// Ошибки валидации
		{
			name:        "empty_field_type",
			field:       operation.Field{Name: "test"},
			value:       "test_value",
			expectError: true,
			errorMsg:    "field type is required",
		},
		{
			name: "nil_value",
			field: operation.Field{
				Name: "test",
				Type: operation.FieldTypeString,
			},
			value:       nil,
			expectError: true,
			errorMsg:    "value is required",
		},
		{
			name: "unknown_field_type",
			field: operation.Field{
				Name: "test",
				Type: "unknown_type",
			},
			value:       "test_value",
			expectError: true,
			errorMsg:    "validator not found for field type: unknown_type",
		},

		// String валидация
		{
			name: "string_success",
			field: operation.Field{
				Name: "test_string",
				Type: operation.FieldTypeString,
			},
			value:       "test_value",
			expectError: false,
		},
		{
			name: "string_invalid_type",
			field: operation.Field{
				Name: "test_string",
				Type: operation.FieldTypeString,
			},
			value:       123,
			expectError: true,
			errorMsg:    "field \"test_string\" is not a string",
		},
		{
			name: "string_empty",
			field: operation.Field{
				Name: "empty_string",
				Type: operation.FieldTypeString,
			},
			value:       "",
			expectError: false,
		},

		// Int64 валидация
		{
			name: "int64_success",
			field: operation.Field{
				Name: "test_int64",
				Type: operation.FieldTypeInt64,
			},
			value:       int64(123),
			expectError: false,
		},
		{
			name: "int64_as_float64_success",
			field: operation.Field{
				Name: "test_int64",
				Type: operation.FieldTypeInt64,
			},
			value:       float64(123),
			expectError: false,
		},
		{
			name: "int64_invalid_type",
			field: operation.Field{
				Name: "test_int64",
				Type: operation.FieldTypeInt64,
			},
			value:       "not_a_number",
			expectError: true,
			errorMsg:    "field \"test_int64\" is not a float64",
		},
		{
			name: "int64_zero_value",
			field: operation.Field{
				Name: "zero_int64",
				Type: operation.FieldTypeInt64,
			},
			value:       int64(0),
			expectError: false,
		},

		// Float64 валидация
		{
			name: "float64_success",
			field: operation.Field{
				Name: "test_float64",
				Type: operation.FieldTypeFloat64,
			},
			value:       float64(123.45),
			expectError: false,
		},
		{
			name: "float64_invalid_type",
			field: operation.Field{
				Name: "test_float64",
				Type: operation.FieldTypeFloat64,
			},
			value:       "not_a_float",
			expectError: true,
			errorMsg:    "field \"test_float64\" is not a float64",
		},
		{
			name: "float64_zero_value",
			field: operation.Field{
				Name: "zero_float64",
				Type: operation.FieldTypeFloat64,
			},
			value:       float64(0.0),
			expectError: false,
		},

		// Bool валидация
		{
			name: "bool_success_true",
			field: operation.Field{
				Name: "test_bool",
				Type: operation.FieldTypeBool,
			},
			value:       true,
			expectError: false,
		},
		{
			name: "bool_success_false",
			field: operation.Field{
				Name: "test_bool",
				Type: operation.FieldTypeBool,
			},
			value:       false,
			expectError: false,
		},
		{
			name: "bool_invalid_type",
			field: operation.Field{
				Name: "test_bool",
				Type: operation.FieldTypeBool,
			},
			value:       "not_a_bool",
			expectError: true,
			errorMsg:    "field \"test_bool\" is not a bool",
		},

		// UUID валидация
		{
			name: "uuid_success",
			field: operation.Field{
				Name: "test_uuid",
				Type: operation.FieldTypeUUID,
			},
			value:       testUUID,
			expectError: false,
		},
		{
			name: "uuid_invalid_type",
			field: operation.Field{
				Name: "test_uuid",
				Type: operation.FieldTypeUUID,
			},
			value:       "not_a_uuid",
			expectError: true,
			errorMsg:    "field \"test_uuid\" is not a uuid",
		},
		{
			name: "uuid_nil_value",
			field: operation.Field{
				Name: "nil_uuid",
				Type: operation.FieldTypeUUID,
			},
			value:       uuid.Nil,
			expectError: false,
		},

		// Chained calls test
		{
			name: "chained_calls_success",
			field: operation.Field{
				Name: "chained_test",
				Type: operation.FieldTypeString,
			},
			value:       "test_value",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := New().
				WithField(tc.field).
				WithVal(tc.value)

			err := v.Validate()

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestForField(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		expectError bool
		errorMsg    string
	}{
		// Валидные типы полей
		{
			name:        "string_type",
			field:       operation.Field{Type: operation.FieldTypeString},
			expectError: false,
		},
		{
			name:        "int64_type",
			field:       operation.Field{Type: operation.FieldTypeInt64},
			expectError: false,
		},
		{
			name:        "float64_type",
			field:       operation.Field{Type: operation.FieldTypeFloat64},
			expectError: false,
		},
		{
			name:        "bool_type",
			field:       operation.Field{Type: operation.FieldTypeBool},
			expectError: false,
		},
		{
			name:        "uuid_type",
			field:       operation.Field{Type: operation.FieldTypeUUID},
			expectError: false,
		},

		// Невалидные типы полей
		{
			name:        "invalid_type",
			field:       operation.Field{Type: "invalid_type"},
			expectError: true,
			errorMsg:    "validator not found for field type: invalid_type",
		},
		{
			name:        "empty_type",
			field:       operation.Field{Type: ""},
			expectError: true,
			errorMsg:    "validator not found for field type: ",
		},
		{
			name:        "unknown_type",
			field:       operation.Field{Type: "unknown_type"},
			expectError: true,
			errorMsg:    "validator not found for field type: unknown_type",
		},
		{
			name:        "numeric_type",
			field:       operation.Field{Type: "numeric"},
			expectError: true,
			errorMsg:    "validator not found for field type: numeric",
		},
		{
			name:        "text_type",
			field:       operation.Field{Type: "text"},
			expectError: true,
			errorMsg:    "validator not found for field type: text",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			validator, err := forField(tc.field)

			if tc.expectError {
				require.Error(t, err)
				assert.Nil(t, validator)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, validator)
			}
		})
	}
}

// Тесты для отдельных функций валидации.
//
//nolint:funlen // тестовая функция
func TestValidateString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeString},
			value:       "test_value",
			expectError: false,
		},
		{
			name:        "empty_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeString},
			value:       "",
			expectError: false,
		},
		{
			name:        "long_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeString},
			value:       "this is a very long string with many characters",
			expectError: false,
		},
		{
			name:        "invalid_type_int",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeString},
			value:       123,
			expectError: true,
			errorMsg:    "field \"test\" is not a string",
		},
		{
			name:        "invalid_type_bool",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeString},
			value:       true,
			expectError: true,
			errorMsg:    "field \"test\" is not a string",
		},
		{
			name:        "invalid_type_float",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeString},
			value:       float64(123.45),
			expectError: true,
			errorMsg:    "field \"test\" is not a string",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateString(tc.field, tc.value)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestValidateInt64(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_int64",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       int64(123),
			expectError: false,
		},
		{
			name:        "valid_int64_zero",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       int64(0),
			expectError: false,
		},
		{
			name:        "valid_int64_negative",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       int64(-123),
			expectError: false,
		},
		{
			name:        "valid_float64_fallback",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       float64(123),
			expectError: false,
		},
		{
			name:        "valid_float64_zero_fallback",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       float64(0),
			expectError: false,
		},
		{
			name:        "invalid_type_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       "not_a_number",
			expectError: true,
			errorMsg:    "field \"test\" is not a float64",
		},
		{
			name:        "invalid_type_bool",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeInt64},
			value:       true,
			expectError: true,
			errorMsg:    "field \"test\" is not a float64",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateInt64(tc.field, tc.value)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestValidateFloat64(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_float64",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       float64(123.45),
			expectError: false,
		},
		{
			name:        "valid_float64_zero",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       float64(0.0),
			expectError: false,
		},
		{
			name:        "valid_float64_negative",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       float64(-123.45),
			expectError: false,
		},
		{
			name:        "valid_float64_large",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       float64(999999.999),
			expectError: false,
		},
		{
			name:        "invalid_type_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       "not_a_float",
			expectError: true,
			errorMsg:    "field \"test\" is not a float64",
		},
		{
			name:        "invalid_type_int",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       123,
			expectError: true,
			errorMsg:    "field \"test\" is not a float64",
		},
		{
			name:        "invalid_type_bool",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeFloat64},
			value:       true,
			expectError: true,
			errorMsg:    "field \"test\" is not a float64",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateFloat64(tc.field, tc.value)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestValidateBool(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_bool_true",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeBool},
			value:       true,
			expectError: false,
		},
		{
			name:        "valid_bool_false",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeBool},
			value:       false,
			expectError: false,
		},
		{
			name:        "invalid_type_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeBool},
			value:       "not_a_bool",
			expectError: true,
			errorMsg:    "field \"test\" is not a bool",
		},
		{
			name:        "invalid_type_int",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeBool},
			value:       123,
			expectError: true,
			errorMsg:    "field \"test\" is not a bool",
		},
		{
			name:        "invalid_type_float",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeBool},
			value:       float64(123.45),
			expectError: true,
			errorMsg:    "field \"test\" is not a bool",
		},
		{
			name:        "invalid_type_string_true",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeBool},
			value:       "true",
			expectError: true,
			errorMsg:    "field \"test\" is not a bool",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateBool(tc.field, tc.value)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestValidateUUID(t *testing.T) {
	t.Parallel()

	testUUID := uuid.New()

	testCases := []struct {
		name        string
		field       operation.Field
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_uuid",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       testUUID,
			expectError: false,
		},
		{
			name:        "valid_uuid_nil",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       uuid.Nil,
			expectError: false,
		},
		{
			name:        "invalid_type_string",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       "not_a_uuid",
			expectError: true,
			errorMsg:    "field \"test\" is not a uuid",
		},
		{
			name:        "invalid_type_int",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       123,
			expectError: true,
			errorMsg:    "field \"test\" is not a uuid",
		},
		{
			name:        "invalid_type_bool",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       true,
			expectError: true,
			errorMsg:    "field \"test\" is not a uuid",
		},
		{
			name:        "invalid_type_float",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       float64(123.45),
			expectError: true,
			errorMsg:    "field \"test\" is not a uuid",
		},
		{
			name:        "invalid_type_string_uuid",
			field:       operation.Field{Name: "test", Type: operation.FieldTypeUUID},
			value:       "550e8400-e29b-41d4-a716-446655440000",
			expectError: true,
			errorMsg:    "field \"test\" is not a uuid",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateUUID(tc.field, tc.value)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatorsMap_Completeness(t *testing.T) {
	t.Parallel()

	// Проверяем, что все типы полей имеют соответствующие валидаторы
	expectedTypes := []operation.FieldType{
		operation.FieldTypeString,
		operation.FieldTypeInt64,
		operation.FieldTypeFloat64,
		operation.FieldTypeBool,
		operation.FieldTypeUUID,
	}

	for _, fieldType := range expectedTypes {
		t.Run(string(fieldType), func(t *testing.T) {
			t.Parallel()

			validator, exists := validatorsMap[fieldType]
			assert.True(t, exists, "Validator for type %s should exist", fieldType)
			assert.NotNil(t, validator, "Validator for type %s should not be nil", fieldType)
		})
	}
}
