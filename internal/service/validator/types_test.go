package validator

import (
	"db-worker/internal/config/operation"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // тестовая функция
func TestValidateInt64Value(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       int64
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid_value_no_validation",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
			},
			value:       int64(123),
			expectError: false,
		},
		{
			name: "valid_value_zero",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
			},
			value:       int64(0),
			expectError: false,
		},
		{
			name: "valid_value_negative",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
			},
			value:       int64(-123),
			expectError: false,
		},
		{
			name: "expected_value_match",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: int64(123),
				},
			},
			value:       int64(123),
			expectError: false,
		},
		{
			name: "expected_value_mismatch",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: int64(123),
				},
			},
			value:       int64(456),
			expectError: true,
			errorMsg:    "field \"test_field\" must be 123, but got 456",
		},
		{
			name: "max_value_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					Max: fromValToPointer(t, 100),
				},
			},
			value:       int64(50),
			expectError: false,
		},
		{
			name: "max_value_equal",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					Max: fromValToPointer(t, 100),
				},
			},
			value:       int64(100),
			expectError: false,
		},
		{
			name: "max_value_exceeded",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					Max: fromValToPointer(t, 100),
				},
			},
			value:       int64(150),
			expectError: true,
			errorMsg:    "field \"test_field\" must be less than 100, but got 150",
		},
		{
			name: "min_value_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					Min: fromValToPointer(t, 10),
				},
			},
			value:       int64(50),
			expectError: false,
		},
		{
			name: "min_value_equal",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					Min: fromValToPointer(t, 10),
				},
			},
			value:       int64(10),
			expectError: false,
		},
		{
			name: "min_value_below",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					Min: fromValToPointer(t, 10),
				},
			},
			value:       int64(5),
			expectError: true,
			errorMsg:    "field \"test_field\" must be greater than 10, but got 5",
		},
		{
			name: "combined_validation_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: int64(50),
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 100),
				},
			},
			value:       int64(50),
			expectError: false,
		},
		{
			name: "combined_validation_invalid_expected",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeInt64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: int64(50),
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 100),
				},
			},
			value:       int64(75),
			expectError: true,
			errorMsg:    "field \"test_field\" must be 50, but got 75",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateInt64Value(tc.field, tc.value)

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
func TestValidateFloat64Value(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       float64
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid_value_no_validation",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
			},
			value:       float64(123.45),
			expectError: false,
		},
		{
			name: "valid_value_zero",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
			},
			value:       float64(0.0),
			expectError: false,
		},
		{
			name: "valid_value_negative",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
			},
			value:       float64(-123.45),
			expectError: false,
		},
		{
			name: "expected_value_float64_match",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: float64(123.45),
				},
			},
			value:       float64(123.45),
			expectError: false,
		},
		{
			name: "expected_value_int_match",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: int(123),
				},
			},
			value:       float64(123.0),
			expectError: false,
		},
		{
			name: "expected_value_float64_mismatch",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: float64(123.45),
				},
			},
			value:       float64(456.78),
			expectError: true,
			errorMsg:    "field \"test_field\" must be 123.450000, but got 456.780000",
		},
		{
			name: "expected_value_invalid_type",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "invalid",
				},
			},
			value:       float64(123.45),
			expectError: true,
			errorMsg:    "field \"test_field\": invalid type of expected value",
		},
		{
			name: "max_value_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					Max: fromValToPointer(t, 100),
				},
			},
			value:       float64(50.5),
			expectError: false,
		},
		{
			name: "max_value_equal",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					Max: fromValToPointer(t, 100),
				},
			},
			value:       float64(100.0),
			expectError: false,
		},
		{
			name: "max_value_exceeded",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					Max: fromValToPointer(t, 100),
				},
			},
			value:       float64(150.5),
			expectError: true,
			errorMsg:    "field \"test_field\" must be less than 100, but got 150.500000",
		},
		{
			name: "min_value_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					Min: fromValToPointer(t, 10),
				},
			},
			value:       float64(50.5),
			expectError: false,
		},
		{
			name: "min_value_equal",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					Min: fromValToPointer(t, 10),
				},
			},
			value:       float64(10.0),
			expectError: false,
		},
		{
			name: "min_value_below",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					Min: fromValToPointer(t, 10),
				},
			},
			value:       float64(5.5),
			expectError: true,
			errorMsg:    "field \"test_field\" must be greater than 10, but got 5.500000",
		},
		{
			name: "combined_validation_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeFloat64,
				Validation: operation.AggregatedValidation{
					ExpectedValue: float64(50.5),
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 100),
				},
			},
			value:       float64(50.5),
			expectError: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateFloat64Value(tc.field, tc.value)

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
func TestValidateStringValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid_value_no_validation",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
			},
			value:       "test_value",
			expectError: false,
		},
		{
			name: "valid_value_empty",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
			},
			value:       "",
			expectError: false,
		},
		{
			name: "expected_value_match",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "expected_value",
				},
			},
			value:       "expected_value",
			expectError: false,
		},
		{
			name: "expected_value_mismatch",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "expected_value",
				},
			},
			value:       "different_value",
			expectError: true,
			errorMsg:    "field \"test_field\" must be expected_value, but got different_value",
		},
		{
			name: "not_empty_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					NotEmpty: true,
				},
			},
			value:       "non_empty_string",
			expectError: false,
		},
		{
			name: "not_empty_invalid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					NotEmpty: true,
				},
			},
			value:       "",
			expectError: true,
			errorMsg:    "field \"test_field\" must be not empty",
		},
		{
			name: "max_length_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MaxLength: fromValToPointer(t, 10),
				},
			},
			value:       "short",
			expectError: false,
		},
		{
			name: "max_length_equal",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MaxLength: fromValToPointer(t, 10),
				},
			},
			value:       "1234567890",
			expectError: false,
		},
		{
			name: "max_length_exceeded",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MaxLength: fromValToPointer(t, 10),
				},
			},
			value:       "very_long_string",
			expectError: true,
			errorMsg:    "length of field \"test_field\" must be less than 10, but got 16",
		},
		{
			name: "min_length_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MinLength: fromValToPointer(t, 5),
				},
			},
			value:       "longer",
			expectError: false,
		},
		{
			name: "min_length_equal",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MinLength: fromValToPointer(t, 5),
				},
			},
			value:       "12345",
			expectError: false,
		},
		{
			name: "min_length_below",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MinLength: fromValToPointer(t, 5),
				},
			},
			value:       "hi",
			expectError: true,
			errorMsg:    "length of field \"test_field\" must be greater than 5, but got 2",
		},
		{
			name: "combined_validation_valid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "test",
					NotEmpty:      true,
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 10),
				},
			},
			value:       "test",
			expectError: false,
		},
		{
			name: "combined_validation_invalid_expected",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "test",
					NotEmpty:      true,
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 10),
				},
			},
			value:       "different",
			expectError: true,
			errorMsg:    "field \"test_field\" must be test, but got different",
		},
		{
			name: "combined_validation_invalid_empty",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "test",
					NotEmpty:      true,
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 10),
				},
			},
			value:       "",
			expectError: true,
			errorMsg:    "field \"test_field\" must be test, but got ",
		},
		{
			name: "combined_validation_invalid_length",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					ExpectedValue: "test",
					NotEmpty:      true,
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 10),
				},
			},
			value:       "very_long_string_exceeding_max_length",
			expectError: true,
			errorMsg:    "field \"test_field\" must be test, but got very_long_string_exceeding_max_length",
		},
		{
			name: "not_empty_only_invalid",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					NotEmpty: true,
				},
			},
			value:       "",
			expectError: true,
			errorMsg:    "field \"test_field\" must be not empty",
		},
		{
			name: "max_length_only_exceeded",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MaxLength: fromValToPointer(t, 5),
				},
			},
			value:       "very_long_string",
			expectError: true,
			errorMsg:    "length of field \"test_field\" must be less than 5, but got 16",
		},
		{
			name: "min_length_only_below",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeString,
				Validation: operation.AggregatedValidation{
					MinLength: fromValToPointer(t, 10),
				},
			},
			value:       "short",
			expectError: true,
			errorMsg:    "length of field \"test_field\" must be greater than 10, but got 5",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateStringValue(tc.field, tc.value)

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
func TestValidateBoolValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		field       operation.Field
		value       bool
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid_value_no_validation",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeBool,
			},
			value:       true,
			expectError: false,
		},
		{
			name: "valid_value_false",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeBool,
			},
			value:       false,
			expectError: false,
		},
		{
			name: "expected_value_true_match",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeBool,
				Validation: operation.AggregatedValidation{
					ExpectedValue: true,
				},
			},
			value:       true,
			expectError: false,
		},
		{
			name: "expected_value_false_match",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeBool,
				Validation: operation.AggregatedValidation{
					ExpectedValue: false,
				},
			},
			value:       false,
			expectError: false,
		},
		{
			name: "expected_value_true_mismatch",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeBool,
				Validation: operation.AggregatedValidation{
					ExpectedValue: true,
				},
			},
			value:       false,
			expectError: true,
			errorMsg:    "field \"test_field\" must be true, but got false",
		},
		{
			name: "expected_value_false_mismatch",
			field: operation.Field{
				Name: "test_field",
				Type: operation.FieldTypeBool,
				Validation: operation.AggregatedValidation{
					ExpectedValue: false,
				},
			},
			value:       true,
			expectError: true,
			errorMsg:    "field \"test_field\" must be false, but got true",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateBoolValue(tc.field, tc.value)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func fromValToPointer[T any](t *testing.T, val T) *T {
	t.Helper()
	return &val
}
