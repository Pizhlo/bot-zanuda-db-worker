package operation

import (
	"db-worker/internal/config/operation"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestValidateMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     *Service
		msg     map[string]interface{}
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
				mapFields: map[string]operation.Field{
					"field1": {
						Name: "field1",
						Type: "string",
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: missing required field",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
				mapFields: map[string]operation.Field{
					"field1": {
						Name: "field1",
						Type: "string",
					},
				},
			},
			msg:     map[string]interface{}{},
			wantErr: require.Error,
		},
		{
			name: "negative case: wrong field type",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
				mapFields: map[string]operation.Field{
					"field1": {
						Name: "field1",
						Type: "string",
					},
				},
			},
			msg: map[string]interface{}{
				"field1": 123,
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: multiple fields with different types",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name:     "field1",
							Type:     "string",
							Required: true,
						},
						{
							Name:     "field2",
							Type:     "int64",
							Required: true,
						},
						{
							Name:     "field3",
							Type:     "float64",
							Required: false,
						},
					},
				},
				mapFields: map[string]operation.Field{
					"field1": {
						Name: "field1",
						Type: "string",
					},
					"field2": {
						Name: "field2",
						Type: "int64",
					},
					"field3": {
						Name: "field3",
						Type: "float64",
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
				"field2": int64(123),
				"field3": 123.45,
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.validateMessage(tt.msg)
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateFieldsCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     *Service
		msg     map[string]interface{}
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: all required fields present",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name:     "field1",
							Type:     "string",
							Required: true,
						},
						{
							Name:     "field2",
							Type:     "int64",
							Required: true,
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
				"field2": int64(123),
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: optional fields missing",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name:     "field1",
							Type:     "string",
							Required: true,
						},
						{
							Name:     "field2",
							Type:     "int64",
							Required: false,
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: missing required field",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name:     "field1",
							Type:     "string",
							Required: true,
						},
						{
							Name:     "field2",
							Type:     "int64",
							Required: true,
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: empty message",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name:     "field1",
							Type:     "string",
							Required: true,
						},
					},
				},
			},
			msg:     map[string]interface{}{},
			wantErr: require.Error,
		},
		{
			name: "negative case: nil message",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name:     "field1",
							Type:     "string",
							Required: true,
						},
					},
				},
			},
			msg:     nil,
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.validateFieldsCount(tt.msg)
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateFieldVals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     *Service
		msg     map[string]interface{}
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: all field types valid",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
						{
							Name: "field2",
							Type: "int64",
						},
						{
							Name: "field3",
							Type: "float64",
						},
						{
							Name: "field4",
							Type: "bool",
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
				"field2": int64(123),
				"field3": 123.45,
				"field4": true,
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: int64 as float64 (conversion)",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "int64",
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": 123.0, // float64 that represents int64
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: field not found",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field2": "test", // wrong field name
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: wrong field type",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": 123, // int instead of string
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: unsupported field type",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "unknown",
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: nil value",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
			},
			msg: map[string]interface{}{
				"field1": nil,
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.validateFieldVals(tt.msg)
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateFieldVal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     *Service
		field   operation.Field
		value   interface{}
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case: string type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "string",
			},
			value:   "test",
			wantErr: require.NoError,
		},
		{
			name: "positive case: int64 type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "int64",
			},
			value:   int64(123),
			wantErr: require.NoError,
		},
		{
			name: "positive case: int64 as float64",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "int64",
			},
			value:   123.0, // float64 representing int64
			wantErr: require.NoError,
		},
		{
			name: "positive case: float64 type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "float64",
			},
			value:   123.45,
			wantErr: require.NoError,
		},
		{
			name: "positive case: bool type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "bool",
			},
			value:   true,
			wantErr: require.NoError,
		},
		{
			name: "positive case: uuid type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "uuid",
			},
			value:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), // valid UUID
			wantErr: require.NoError,
		},
		{
			name: "negative case: wrong type for string",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "string",
			},
			value:   123,
			wantErr: require.Error,
		},
		{
			name: "negative case: wrong type for int64",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "int64",
			},
			value:   "123",
			wantErr: require.Error,
		},
		{
			name: "negative case: wrong type for float64",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "float64",
			},
			value:   "123.45",
			wantErr: require.Error,
		},
		{
			name: "negative case: wrong type for bool",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "bool",
			},
			value:   "true",
			wantErr: require.Error,
		},
		{
			name: "negative case: wrong type for uuid",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "uuid",
			},
			value:   123,
			wantErr: require.Error,
		},
		{
			name: "negative case: nil value",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "string",
			},
			value:   nil,
			wantErr: require.Error,
		},
		{
			name: "negative case: unsupported field type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "unknown",
			},
			value:   "test",
			wantErr: require.Error,
		},
		{
			name: "negative case: empty field type",
			svc: &Service{
				cfg: &operation.Operation{
					Name:   "test",
					Fields: []operation.Field{},
				},
			},
			field: operation.Field{
				Name: "field1",
				Type: "",
			},
			value:   "test",
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.validateFieldVal(tt.value, tt.field)
			tt.wantErr(t, err)
		})
	}
}
