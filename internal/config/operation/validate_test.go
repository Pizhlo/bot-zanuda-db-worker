package operation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestValidateRuleCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					Max: fromValToPointer(t, 10),
					Min: fromValToPointer(t, 3),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "string: max",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					Max: fromValToPointer(t, 10),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "string: min",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "int64: max_length",
			f: Field{
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					MaxLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "int64: min_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "float64: max_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					MaxLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "float64: min_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "uuid: max",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					MaxLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "uuid: min",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bool: max",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					Max: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bool: min",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bool: max_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					MaxLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bool: min_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bool: not_empty",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					NotEmpty: true,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bool: value",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					ExpectedValue: true,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "valid int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 3),
					Max: fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "valid float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 3),
					Max: fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "valid uuid",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: fromValToPointer(t, "123e4567-e89b-12d3-a456-426614174000"),
					NotEmpty:      true,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "valid bool",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					ExpectedValue: true,
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateRuleCompatibility(test.f)
			test.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateBoundaryConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 3),
					Max: fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 3),
					Max: fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 10),
					Max: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					Min: fromValToPointer(t, 10),
					Max: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: string",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 3),
					MaxLength: fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: string",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 10),
					MaxLength: fromValToPointer(t, 3),
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateBoundaryConsistency(test.f)
			test.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateExpectedValueConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: nil",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: nil,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: string",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: string: expected value is not string",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: string: shorter than min_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 10),
					MaxLength:     fromValToPointer(t, 11),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: string: longer than max_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 4),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: 5,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: int64: expected value is not int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: int64: value is less than min",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: 9,
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 11),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: int64: value is greater than max",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 4),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 5.5,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: float64: expected value is not float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: float64: value is less than min",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 9.5,
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 11),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: float64: value is greater than max",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 123.5,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 4),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: uuid",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: "123e4567-e89b-12d3-a456-426614174000",
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: uuid: expected value is not uuid",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "unsupported case: unsupported field type",
			f: Field{
				Name: "field1",
				Type: "unsupported",
				Validation: AggregatedValidation{
					ExpectedValue: "string",
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateExpectedValueConsistency(test.f)
			test.wantErr(t, err)
		})
	}
}

//nolint:dupl,funlen // проверяем одни случаи в разных тестах; неважно на длину тестовой функции
func TestValidateExpectedValueString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: string",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: string: expected value is not string",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: string: shorter than min_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 10),
					MaxLength:     fromValToPointer(t, 11),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: string: longer than max_length",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 3),
					MaxLength:     fromValToPointer(t, 4),
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateExpectedValueString(test.f)
			test.wantErr(t, err)
		})
	}
}

//nolint:dupl,funlen // проверяем одни случаи в разных тестах; неважно на длину тестовой функции
func TestValidateExpectedValueInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: 5,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: int64: expected value is not int64",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: int64: value is less than min",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: 9,
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 11),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: int64: value is greater than max",
			f: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 4),
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateExpectedValueInt64(test.f)
			test.wantErr(t, err)
		})
	}
}

//nolint:dupl,funlen // проверяем одни случаи в разных тестах; неважно на длину тестовой функции
func TestValidateExpectedValueFloat64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 5.5,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 10),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: float64: expected value is not float64",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: float64: value is less than min",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 9.5,
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 11),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: float64: value is greater than max",
			f: Field{
				Name: "field1",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 123.5,
					Min:           fromValToPointer(t, 3),
					Max:           fromValToPointer(t, 4),
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateExpectedValueFloat64(test.f)
			test.wantErr(t, err)
		})
	}
}

func TestValidateExpectedValueUUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: uuid",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: "123e4567-e89b-12d3-a456-426614174000",
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: uuid: expected value is not uuid",
			f: Field{
				Name: "field1",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateExpectedValueUUID(test.f)
			test.wantErr(t, err)
		})
	}
}

func fromValToPointer[T any](t *testing.T, val T) *T {
	t.Helper()
	return &val
}
