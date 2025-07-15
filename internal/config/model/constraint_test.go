package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConstraint(t *testing.T) {
	actual := newConstraintValidator()

	require.NotNil(t, actual)
	require.NotNil(t, actual.handlers)
	require.Len(t, actual.handlers, 5)
}

func TestConstraint_Min(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      interface{}
		constraint ValidationConstraint
		wantErr    bool
	}{
		{
			name:  "success int",
			value: 10,
			constraint: ValidationConstraint{
				Min: 10,
			},
			wantErr: false,
		},
		{
			name:  "error int",
			value: 9,
			constraint: ValidationConstraint{
				Min: 10,
			},
			wantErr: true,
		},
		{
			name:  "success int64",
			value: int64(10),
			constraint: ValidationConstraint{
				Min: 10,
			},
			wantErr: false,
		},
		{
			name:  "error int64",
			value: int64(9),
			constraint: ValidationConstraint{
				Min: 10,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := newConstraintValidator().min(tt.value, tt.constraint)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstraint_Max(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      interface{}
		constraint ValidationConstraint
		wantErr    bool
	}{
		{
			name:  "success int",
			value: 10,
			constraint: ValidationConstraint{
				Max: 10,
			},
			wantErr: false,
		},
		{
			name:  "error int",
			value: 11,
			constraint: ValidationConstraint{
				Max: 10,
			},
			wantErr: true,
		},
		{
			name:  "success int64",
			value: int64(9),
			constraint: ValidationConstraint{
				Max: 10,
			},
			wantErr: false,
		},
		{
			name:  "error int64",
			value: int64(12),
			constraint: ValidationConstraint{
				Max: 10,
			},
			wantErr: true,
		},
	}

	c := newConstraintValidator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := c.max(tt.value, tt.constraint)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstraint_MaxLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      interface{}
		constraint ValidationConstraint
		wantErr    bool
	}{
		{
			name:  "success string",
			value: "1234567890",
			constraint: ValidationConstraint{
				MaxLength: 10,
			},
			wantErr: false,
		},
		{
			name:  "error string",
			value: "12345678901",
			constraint: ValidationConstraint{
				MaxLength: 10,
			},
			wantErr: true,
		},
		{
			name:  "not string",
			value: 10,
			constraint: ValidationConstraint{
				MaxLength: 10,
			},
			wantErr: false,
		},
	}

	c := newConstraintValidator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := c.maxLength(tt.value, tt.constraint)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstraint_Enum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      interface{}
		constraint ValidationConstraint
		wantErr    bool
	}{
		{
			name:  "string: success",
			value: "1",
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "3"},
			},
			wantErr: false,
		},
		{
			name:  "string: error",
			value: "4",
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "3"},
			},
			wantErr: true,
		},
		{
			name:  "int: success",
			value: 10,
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10"},
			},
			wantErr: false,
		},
		{
			name:  "int: invalid enum value",
			value: 10,
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc"},
			},
			wantErr: true,
		},
		{
			name:  "int: not matches enum",
			value: 10,
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100"},
			},
			wantErr: true,
		},
		{
			name:  "int8: success",
			value: int8(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10", "100"},
			},
			wantErr: false,
		},
		{
			name:  "int8: invalid enum value",
			value: int8(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int8: not matches enum",
			value: int8(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int16: success",
			value: int16(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10", "100"},
			},
		},
		{
			name:  "int16: invalid enum value",
			value: int16(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int16: not matches enum",
			value: int16(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int32: success",
			value: int32(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10", "100"},
			},
		},
		{
			name:  "int32: invalid enum value",
			value: int32(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int32: not matches enum",
			value: int32(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int64: success",
			value: int64(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10", "100"},
			},
		},
		{
			name:  "int64: invalid enum value",
			value: int64(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "int64: not matches enum",
			value: int64(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "float32: success",
			value: float32(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10", "100"},
			},
		},
		{
			name:  "float32: invalid enum value",
			value: float32(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "float32: not matches enum",
			value: float32(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "float64: success",
			value: float64(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "10", "100"},
			},
		},
		{
			name:  "float64: invalid enum value",
			value: float64(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "4abc", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "float64: not matches enum",
			value: float64(10),
			constraint: ValidationConstraint{
				Enum: []string{"1", "2", "100", "1000"},
			},
			wantErr: true,
		},
		{
			name:  "bool: success",
			value: true,
			constraint: ValidationConstraint{
				Enum: []string{"true", "false"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := newConstraintValidator().enum(tt.value, tt.constraint)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstraint_NotEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		{
			name:    "success string",
			value:   "1",
			wantErr: false,
		},
		{
			name:    "error string",
			value:   "",
			wantErr: true,
		},
		{
			name:    "success slice",
			value:   []string{"1", "2", "3"},
			wantErr: false,
		},
		{
			name:    "error slice",
			value:   []string{},
			wantErr: true,
		},
		{
			name:    "success map",
			value:   map[string]string{"1": "1", "2": "2", "3": "3"},
			wantErr: false,
		},
		{
			name:    "error map",
			value:   map[string]string{},
			wantErr: true,
		},
		{
			name:    "success bool",
			value:   true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := newConstraintValidator().notEmpty(tt.value, ValidationConstraint{})
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
