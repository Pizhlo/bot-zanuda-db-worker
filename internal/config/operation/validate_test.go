package operation

import (
	"reflect"
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
			name: "positive case: bool",
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
			name: "negative case: bool: expected value is not bool",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					ExpectedValue: 123,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: bool: expected value is not bool",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					ExpectedValue: "string",
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

func TestValidateExpectedValueBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: bool",
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
			name: "negative case: bool: expected value is not bool",
			f: Field{
				Name: "field1",
				Type: FieldTypeBool,
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

			err := validateExpectedValueBool(test.f)
			test.wantErr(t, err)
		})
	}
}

// TestValidateFieldConfig тестирует основную функцию валидации полей
//
//nolint:funlen // тестовая функция
func TestValidateFieldConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       Field
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: valid field",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 3),
					MaxLength: fromValToPointer(t, 10),
					NotEmpty:  true,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: rule compatibility error",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					Max: fromValToPointer(t, 10), // недопустимо для string
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: boundary consistency error",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 10),
					MaxLength: fromValToPointer(t, 3), // max < min
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: expected value consistency error",
			f: Field{
				Name: "field1",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: 123, // не string
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateFieldConfig(test.f)
			test.wantErr(t, err)
		})
	}
}

// TestValidateWhereCondition тестирует валидацию where условий
//
//nolint:funlen // тестовая функция
func TestValidateWhereCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      Operation
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: create operation without where",
			op: Operation{
				Name:  "test_op",
				Type:  OperationTypeCreate,
				Where: []Where{},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: create operation with where",
			op: Operation{
				Name: "test_op",
				Type: OperationTypeCreate,
				Where: []Where{
					{
						Fields: []WhereField{
							{
								Field: Field{Name: "user_id", Type: FieldTypeInt64},
								Value: int64(123),
							},
						},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: update operation with valid where",
			op: Operation{
				Name: "test_op",
				Type: OperationTypeUpdate,
				FieldsMap: map[string]Field{
					"user_id": {
						Name: "user_id",
						Type: FieldTypeInt64,
						Validation: AggregatedValidation{
							ExpectedValue: int64(123),
						},
					},
				},
				Where: []Where{
					{
						Fields: []WhereField{
							{
								Field: Field{Name: "user_id", Type: FieldTypeInt64},
								Value: int64(123),
							},
						},
					},
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.op.validateWhereCondition()
			test.wantErr(t, err)
		})
	}
}

// TestValidateSingleWhereCondition тестирует валидацию одиночных where условий
//
//nolint:funlen // тестовая функция
func TestValidateSingleWhereCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		w         Where
		fieldsMap map[string]Field
		opName    string
		idx       int
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "negative case: empty fields",
			w: Where{
				Fields: []WhereField{},
			},
			fieldsMap: map[string]Field{},
			opName:    "test_op",
			idx:       0,
			wantErr:   require.Error,
		},
		{
			name: "negative case: single field with type",
			w: Where{
				Type: WhereTypeAnd,
				Fields: []WhereField{
					{
						Field: Field{Name: "user_id", Type: FieldTypeInt64},
						Value: int64(123),
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "negative case: multiple fields without type",
			w: Where{
				Fields: []WhereField{
					{
						Field: Field{Name: "user_id", Type: FieldTypeInt64},
						Value: int64(123),
					},
					{
						Field: Field{Name: "name", Type: FieldTypeString},
						Value: "test",
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
				"name": {
					Name: "name",
					Type: FieldTypeString,
					Validation: AggregatedValidation{
						ExpectedValue: "test",
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: single field without type",
			w: Where{
				Fields: []WhereField{
					{
						Field: Field{Name: "user_id", Type: FieldTypeInt64},
						Value: int64(123),
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "positive case: multiple fields with type",
			w: Where{
				Type: WhereTypeAnd,
				Fields: []WhereField{
					{
						Field: Field{Name: "user_id", Type: FieldTypeInt64},
						Value: int64(123),
					},
					{
						Field: Field{Name: "name", Type: FieldTypeString},
						Value: "test",
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
				"name": {
					Name: "name",
					Type: FieldTypeString,
					Validation: AggregatedValidation{
						ExpectedValue: "test",
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateSingleWhereCondition(test.w, test.fieldsMap, test.opName, test.idx)
			test.wantErr(t, err)
		})
	}
}

// TestValidateWhereField тестирует валидацию полей в where условиях
//
//nolint:funlen // тестовая функция
func TestValidateWhereField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		whereField WhereField
		fieldsMap  map[string]Field
		opName     string
		idx        int
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "negative case: field not found",
			whereField: WhereField{
				Field: Field{Name: "nonexistent", Type: FieldTypeInt64},
				Value: int64(123),
			},
			fieldsMap: map[string]Field{},
			opName:    "test_op",
			idx:       0,
			wantErr:   require.Error,
		},
		{
			name: "negative case: value set but expected value not set",
			whereField: WhereField{
				Field: Field{Name: "user_id", Type: FieldTypeInt64},
				Value: int64(123),
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: nil,
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: no value set",
			whereField: WhereField{
				Field: Field{Name: "user_id", Type: FieldTypeInt64},
				Value: nil,
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: nil,
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "positive case: value matches expected value",
			whereField: WhereField{
				Field: Field{Name: "user_id", Type: FieldTypeInt64},
				Value: int64(123),
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateWhereField(test.whereField, test.fieldsMap, test.opName, test.idx)
			test.wantErr(t, err)
		})
	}
}

// TestValidateMultipleWhereCondition тестирует валидацию множественных where условий
//
//nolint:funlen // тестовая функция
func TestValidateMultipleWhereCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		w         Where
		fieldsMap map[string]Field
		opName    string
		idx       int
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "negative case: empty type",
			w: Where{
				Type: "",
				Conditions: []Where{
					{
						Fields: []WhereField{
							{
								Field: Field{Name: "user_id", Type: FieldTypeInt64},
								Value: int64(123),
							},
						},
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "negative case: fields not empty",
			w: Where{
				Type: WhereTypeAnd,
				Fields: []WhereField{
					{
						Field: Field{Name: "user_id", Type: FieldTypeInt64},
						Value: int64(123),
					},
				},
				Conditions: []Where{
					{
						Fields: []WhereField{
							{
								Field: Field{Name: "user_id", Type: FieldTypeInt64},
								Value: int64(123),
							},
						},
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: valid multiple conditions",
			w: Where{
				Type: WhereTypeAnd,
				Conditions: []Where{
					{
						Fields: []WhereField{
							{
								Field: Field{Name: "user_id", Type: FieldTypeInt64},
								Value: int64(123),
							},
						},
					},
					{
						Fields: []WhereField{
							{
								Field: Field{Name: "name", Type: FieldTypeString},
								Value: "test",
							},
						},
					},
				},
			},
			fieldsMap: map[string]Field{
				"user_id": {
					Name: "user_id",
					Type: FieldTypeInt64,
					Validation: AggregatedValidation{
						ExpectedValue: int64(123),
					},
				},
				"name": {
					Name: "name",
					Type: FieldTypeString,
					Validation: AggregatedValidation{
						ExpectedValue: "test",
					},
				},
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateMultipleWhereCondition(test.w, test.fieldsMap, test.opName, test.idx)
			test.wantErr(t, err)
		})
	}
}

// TestValidateFieldValues тестирует валидацию значений полей
//
//nolint:funlen // тестовая функция
func TestValidateFieldValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		field      Field
		whereField WhereField
		opName     string
		idx        int
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case: string values match",
			field: Field{
				Name: "name",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "test",
				},
			},
			whereField: WhereField{
				Field: Field{Name: "name", Type: FieldTypeString},
				Value: "test",
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "negative case: string values don't match",
			field: Field{
				Name: "name",
				Type: FieldTypeString,
				Validation: AggregatedValidation{
					ExpectedValue: "test",
				},
			},
			whereField: WhereField{
				Field: Field{Name: "name", Type: FieldTypeString},
				Value: "different",
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: int64 values match",
			field: Field{
				Name: "user_id",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: int64(123),
				},
			},
			whereField: WhereField{
				Field: Field{Name: "user_id", Type: FieldTypeInt64},
				Value: int64(123),
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "negative case: int64 values don't match",
			field: Field{
				Name: "user_id",
				Type: FieldTypeInt64,
				Validation: AggregatedValidation{
					ExpectedValue: int64(123),
				},
			},
			whereField: WhereField{
				Field: Field{Name: "user_id", Type: FieldTypeInt64},
				Value: int64(456),
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: float64 values match",
			field: Field{
				Name: "price",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 123.45,
				},
			},
			whereField: WhereField{
				Field: Field{Name: "price", Type: FieldTypeFloat64},
				Value: 123.45,
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "negative case: float64 values don't match",
			field: Field{
				Name: "price",
				Type: FieldTypeFloat64,
				Validation: AggregatedValidation{
					ExpectedValue: 123.45,
				},
			},
			whereField: WhereField{
				Field: Field{Name: "price", Type: FieldTypeFloat64},
				Value: 456.78,
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: bool values match",
			field: Field{
				Name: "active",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					ExpectedValue: true,
				},
			},
			whereField: WhereField{
				Field: Field{Name: "active", Type: FieldTypeBool},
				Value: true,
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "negative case: bool values don't match",
			field: Field{
				Name: "active",
				Type: FieldTypeBool,
				Validation: AggregatedValidation{
					ExpectedValue: true,
				},
			},
			whereField: WhereField{
				Field: Field{Name: "active", Type: FieldTypeBool},
				Value: false,
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
		{
			name: "positive case: uuid values match",
			field: Field{
				Name: "id",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: "123e4567-e89b-12d3-a456-426614174000",
				},
			},
			whereField: WhereField{
				Field: Field{Name: "id", Type: FieldTypeUUID},
				Value: "123e4567-e89b-12d3-a456-426614174000",
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.NoError,
		},
		{
			name: "negative case: uuid values don't match",
			field: Field{
				Name: "id",
				Type: FieldTypeUUID,
				Validation: AggregatedValidation{
					ExpectedValue: "123e4567-e89b-12d3-a456-426614174000",
				},
			},
			whereField: WhereField{
				Field: Field{Name: "id", Type: FieldTypeUUID},
				Value: "987fcdeb-51a2-43d7-b890-123456789abc",
			},
			opName:  "test_op",
			idx:     0,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateFieldValues(test.field, test.whereField, test.opName, test.idx)
			test.wantErr(t, err)
		})
	}
}

// TestCompareValues тестирует функцию сравнения значений.
//
//nolint:funlen // тестовая функция
func TestCompareValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       any
		valueType   reflect.Type
		expectedVal any
		wantErr     require.ErrorAssertionFunc
	}{
		{
			name:        "positive case: string values match",
			value:       "test",
			valueType:   reflect.TypeOf("string"),
			expectedVal: "test",
			wantErr:     require.NoError,
		},
		{
			name:        "negative case: string values don't match",
			value:       "test",
			valueType:   reflect.TypeOf("string"),
			expectedVal: "different",
			wantErr:     require.Error,
		},
		{
			name:        "negative case: value type mismatch",
			value:       "test",
			valueType:   reflect.TypeOf("string"),
			expectedVal: 123,
			wantErr:     require.Error,
		},
		{
			name:        "negative case: expected value type mismatch",
			value:       123,
			valueType:   reflect.TypeOf(int64(0)),
			expectedVal: "test",
			wantErr:     require.Error,
		},
		{
			name:        "positive case: int64 values match",
			value:       int64(123),
			valueType:   reflect.TypeOf(int64(0)),
			expectedVal: int64(123),
			wantErr:     require.NoError,
		},
		{
			name:        "negative case: int64 values don't match",
			value:       int64(123),
			valueType:   reflect.TypeOf(int64(0)),
			expectedVal: int64(456),
			wantErr:     require.Error,
		},
		{
			name:        "positive case: float64 values match",
			value:       123.45,
			valueType:   reflect.TypeOf(float64(0)),
			expectedVal: 123.45,
			wantErr:     require.NoError,
		},
		{
			name:        "negative case: float64 values don't match",
			value:       123.45,
			valueType:   reflect.TypeOf(float64(0)),
			expectedVal: 456.78,
			wantErr:     require.Error,
		},
		{
			name:        "positive case: bool values match",
			value:       true,
			valueType:   reflect.TypeOf(true),
			expectedVal: true,
			wantErr:     require.NoError,
		},
		{
			name:        "negative case: bool values don't match",
			value:       true,
			valueType:   reflect.TypeOf(true),
			expectedVal: false,
			wantErr:     require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var err error

			switch test.valueType {
			case reflect.TypeOf("string"):
				err = compareValues[string](test.value, test.expectedVal)
			case reflect.TypeOf(int64(0)):
				err = compareValues[int64](test.value, test.expectedVal)
			case reflect.TypeOf(float64(0)):
				err = compareValues[float64](test.value, test.expectedVal)
			case reflect.TypeOf(true):
				err = compareValues[bool](test.value, test.expectedVal)
			default:
				t.Fatalf("unsupported value type: %s", test.valueType)
			}

			test.wantErr(t, err)
		})
	}
}

func fromValToPointer[T any](t *testing.T, val T) *T {
	t.Helper()
	return &val
}
