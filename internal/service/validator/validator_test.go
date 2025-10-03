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

func TestValidate_EmptyFieldType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{Name: "test"}).
		WithVal("test_value")

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "field type is required")
}

func TestValidate_NilValue(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test",
			Type: operation.FieldTypeString,
		}).
		WithVal(nil)

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "value is required")
}

func TestValidate_UnknownFieldType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test",
			Type: "unknown_type",
		}).
		WithVal("test_value")

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validator not found for field type: unknown_type")
}

func TestValidateString_Success(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_string",
			Type: operation.FieldTypeString,
		}).
		WithVal("test_value")

	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidateString_InvalidType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_string",
			Type: operation.FieldTypeString,
		}).
		WithVal(123)

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test_string is not a string")
}

func TestValidateInt64_Success(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_int64",
			Type: operation.FieldTypeInt64,
		}).
		WithVal(int64(123))

	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidateInt64_AsFloat64_Success(t *testing.T) {
	// Тест для случая, когда int64 приходит как float64
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_int64",
			Type: operation.FieldTypeInt64,
		}).
		WithVal(float64(123))

	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidateInt64_InvalidType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_int64",
			Type: operation.FieldTypeInt64,
		}).
		WithVal("not_a_number")

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test_int64 is not a float64")
}

func TestValidateFloat64_Success(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_float64",
			Type: operation.FieldTypeFloat64,
		}).
		WithVal(float64(123.45))

	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidateFloat64_InvalidType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_float64",
			Type: operation.FieldTypeFloat64,
		}).
		WithVal("not_a_float")

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test_float64 is not a float64")
}

func TestValidateBool_Success(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_bool",
			Type: operation.FieldTypeBool,
		}).
		WithVal(true)

	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidateBool_InvalidType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_bool",
			Type: operation.FieldTypeBool,
		}).
		WithVal("not_a_bool")

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test_bool is not a bool")
}

func TestValidateUUID_Success(t *testing.T) {
	t.Parallel()

	testUUID := uuid.New()
	v := New().
		WithField(operation.Field{
			Name: "test_uuid",
			Type: operation.FieldTypeUUID,
		}).
		WithVal(testUUID)

	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidateUUID_InvalidType(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "test_uuid",
			Type: operation.FieldTypeUUID,
		}).
		WithVal("not_a_uuid")

	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test_uuid is not a uuid")
}

func TestForField_ValidTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		fieldType operation.FieldType
	}{
		{"string", operation.FieldTypeString},
		{"int64", operation.FieldTypeInt64},
		{"float64", operation.FieldTypeFloat64},
		{"bool", operation.FieldTypeBool},
		{"uuid", operation.FieldTypeUUID},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			field := operation.Field{Type: tc.fieldType}
			validator, err := forField(field)

			assert.NoError(t, err)
			assert.NotNil(t, validator)
		})
	}
}

func TestForField_InvalidType(t *testing.T) {
	t.Parallel()

	field := operation.Field{Type: "invalid_type"}
	validator, err := forField(field)

	require.Error(t, err)
	assert.Nil(t, validator)
	assert.Contains(t, err.Error(), "validator not found for field type: invalid_type")
}

// Тесты для отдельных функций валидации.
func TestValidateString_Function(t *testing.T) {
	t.Parallel()

	field := operation.Field{Name: "test", Type: operation.FieldTypeString}

	// Успешный случай
	err := validateString(field, "test_value")
	assert.NoError(t, err)

	// Ошибочный случай
	err = validateString(field, 123)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test is not a string")
}

func TestValidateInt64_Function(t *testing.T) {
	t.Parallel()

	field := operation.Field{Name: "test", Type: operation.FieldTypeInt64}

	// Успешный случай с int64
	err := validateInt64(field, int64(123))
	assert.NoError(t, err)

	// Успешный случай с float64 (fallback)
	err = validateInt64(field, float64(123))
	assert.NoError(t, err)

	// Ошибочный случай
	err = validateInt64(field, "not_a_number")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test is not a float64")
}

func TestValidateFloat64_Function(t *testing.T) {
	t.Parallel()

	field := operation.Field{Name: "test", Type: operation.FieldTypeFloat64}

	// Успешный случай
	err := validateFloat64(field, float64(123.45))
	assert.NoError(t, err)

	// Ошибочный случай
	err = validateFloat64(field, "not_a_float")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test is not a float64")
}

func TestValidateBool_Function(t *testing.T) {
	t.Parallel()

	field := operation.Field{Name: "test", Type: operation.FieldTypeBool}

	// Успешный случай
	err := validateBool(field, true)
	assert.NoError(t, err)

	err = validateBool(field, false)
	assert.NoError(t, err)

	// Ошибочный случай
	err = validateBool(field, "not_a_bool")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test is not a bool")
}

func TestValidateUUID_Function(t *testing.T) {
	t.Parallel()

	field := operation.Field{Name: "test", Type: operation.FieldTypeUUID}
	testUUID := uuid.New()

	// Успешный случай
	err := validateUUID(field, testUUID)
	assert.NoError(t, err)

	// Ошибочный случай
	err = validateUUID(field, "not_a_uuid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field test is not a uuid")
}

// Тесты для edge cases.
func TestValidate_EmptyString(t *testing.T) {
	t.Parallel()

	v := New().
		WithField(operation.Field{
			Name: "empty_string",
			Type: operation.FieldTypeString,
		}).
		WithVal("")

	err := v.Validate()

	assert.NoError(t, err) // Пустая строка - валидная строка
}

func TestValidate_ZeroValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		fieldType operation.FieldType
		value     any
	}{
		{"zero_int64", operation.FieldTypeInt64, int64(0)},
		{"zero_float64", operation.FieldTypeFloat64, float64(0.0)},
		{"false_bool", operation.FieldTypeBool, false},
		{"nil_uuid", operation.FieldTypeUUID, uuid.Nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := New().
				WithField(operation.Field{
					Name: tc.name,
					Type: tc.fieldType,
				}).
				WithVal(tc.value)

			err := v.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestValidate_ChainedCalls(t *testing.T) {
	t.Parallel()

	// Тест для проверки, что можно вызывать методы в цепочке
	v := New().
		WithField(operation.Field{
			Name: "chained_test",
			Type: operation.FieldTypeString,
		}).
		WithVal("test_value")

	err := v.Validate()

	assert.NoError(t, err)
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
