package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		field   Field
		value   interface{}
		wantErr bool
	}{
		{
			name: "valid string field",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "not_empty"},
				},
			},
			value:   "test",
			wantErr: false,
		},
		{
			name: "empty required string field",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "not_empty"},
				},
			},
			value:   "",
			wantErr: true,
		},
		{
			name: "valid int64 field",
			field: Field{
				Type:     fieldTypeInt64,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "min", Min: 1},
				},
			},
			value:   int64(5),
			wantErr: false,
		},
		{
			name: "int64 field below minimum",
			field: Field{
				Type:     fieldTypeInt64,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "min", Min: 1},
				},
			},
			value:   int64(0),
			wantErr: true,
		},
		{
			name: "valid enum field",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "enum", Enum: []string{"create", "update", "delete"}},
				},
			},
			value:   "create",
			wantErr: false,
		},
		{
			name: "invalid enum value",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "enum", Enum: []string{"create", "update", "delete"}},
				},
			},
			value:   "invalid",
			wantErr: true,
		},
		{
			name: "string field with max length",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "max_length", MaxLength: 10},
				},
			},
			value:   "short",
			wantErr: false,
		},
		{
			name: "string field exceeding max length",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "max_length", MaxLength: 5},
				},
			},
			value:   "too long string",
			wantErr: true,
		},
		{
			name: "empty required int64 field",
			field: Field{
				Type:     fieldTypeInt64,
				Required: true,
			},
			value:   nil,
			wantErr: true,
		},
		{
			name: "empty not required int64 field",
			field: Field{
				Type:     fieldTypeInt64,
				Required: false,
			},
			value:   nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.field.ValidateField(tt.value)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		field   Field
		value   interface{}
		wantErr bool
	}{
		{
			name: "valid string field",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
			},
			value:   "test",
			wantErr: false,
		},
		{
			name: "invalid string field",
			field: Field{
				Type:     fieldTypeString,
				Required: true,
			},
			value:   123,
			wantErr: true,
		},
		{
			name: "valid int field",
			field: Field{
				Type:     fieldTypeInt,
				Required: true,
			},
			value:   123,
			wantErr: false,
		},
		{
			name: "invalid int field",
			field: Field{
				Type:     fieldTypeInt,
				Required: true,
			},
			value:   "test",
			wantErr: true,
		},
		{
			name: "valid int64 field",
			field: Field{
				Type:     fieldTypeInt64,
				Required: true,
			},
			value:   int64(123),
			wantErr: false,
		},
		{
			name: "invalid int64 field",
			field: Field{
				Type:     fieldTypeInt64,
				Required: true,
			},
			value:   "test",
			wantErr: true,
		},
		{
			name: "valid uuid field",
			field: Field{
				Type:     fieldTypeUUID,
				Required: true,
			},
			value:   "123e4567-e89b-12d3-a456-426614174000",
			wantErr: false,
		},
		{
			name: "invalid uuid field #1",
			field: Field{
				Type:     fieldTypeUUID,
				Required: true,
			},
			value:   123,
			wantErr: true,
		},
		{
			name: "invalid uuid field #2",
			field: Field{
				Type:     fieldTypeUUID,
				Required: true,
			},
			value:   "123e4567-e89b-12d3-a456-4266141740",
			wantErr: true,
		},
		{
			name: "valid bool field",
			field: Field{
				Type:     fieldTypeBool,
				Required: true,
			},
			value:   true,
			wantErr: false,
		},
		{
			name: "invalid bool field",
			field: Field{
				Type:     fieldTypeBool,
				Required: true,
			},
			value:   "test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.field.validateType(tt.value)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetFieldValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		field    Field
		value    interface{}
		expected interface{}
	}{
		{
			name: "return provided value",
			field: Field{
				Type:    fieldTypeString,
				Default: "default",
			},
			value:    "provided",
			expected: "provided",
		},
		{
			name: "return default when value is nil",
			field: Field{
				Type:    fieldTypeString,
				Default: "default",
			},
			value:    nil,
			expected: "default",
		},
		{
			name: "return nil when no default and value is nil",
			field: Field{
				Type: fieldTypeString,
			},
			value:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.field.GetFieldValue(tt.value)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewField(t *testing.T) {
	field := newFieldValidator()

	require.NotNil(t, field)
	require.NotNil(t, field.handlers)
	require.Len(t, field.handlers, 5)
}

func TestField_ValidateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		value        interface{}
		expectedType string
		wantErr      require.ErrorAssertionFunc
	}{
		{
			name:         "valid string field",
			value:        "test",
			expectedType: fieldTypeString,
			wantErr:      require.NoError,
		},
		{
			name:         "invalid type",
			value:        "test",
			expectedType: fieldTypeInt,
			wantErr:      require.Error,
		},
		{
			name:         "unknown type",
			value:        "test",
			expectedType: "unknown",
			wantErr:      require.Error,
		},
		{
			name:         "int64 as float64",
			value:        float64(123),
			expectedType: fieldTypeInt64,
			wantErr:      require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.wantErr(t, newFieldValidator().validateType(tt.value, tt.expectedType))
		})
	}
}
