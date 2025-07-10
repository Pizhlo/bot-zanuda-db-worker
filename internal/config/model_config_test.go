package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadModelConfig(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{
			name:    "valid model config",
			file:    "model.yaml",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := LoadModelConfig(tt.file)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, config)
			require.NotEmpty(t, config.Models)
		})
	}
}

func TestField_ValidateField(t *testing.T) {
	tests := []struct {
		name    string
		field   Field
		value   interface{}
		wantErr bool
	}{
		{
			name: "valid string field",
			field: Field{
				Type:     "string",
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
				Type:     "string",
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
				Type:     "int64",
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
				Type:     "int64",
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
				Type:     "string",
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
				Type:     "string",
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
				Type:     "string",
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
				Type:     "string",
				Required: true,
				Validation: []ValidationConstraint{
					{Type: "max_length", MaxLength: 5},
				},
			},
			value:   "too long string",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.field.ValidateField(tt.value)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestField_GetFieldValue(t *testing.T) {
	tests := []struct {
		name     string
		field    Field
		value    interface{}
		expected interface{}
	}{
		{
			name: "return provided value",
			field: Field{
				Type:    "string",
				Default: "default",
			},
			value:    "provided",
			expected: "provided",
		},
		{
			name: "return default when value is nil",
			field: Field{
				Type:    "string",
				Default: "default",
			},
			value:    nil,
			expected: "default",
		},
		{
			name: "return nil when no default and value is nil",
			field: Field{
				Type: "string",
			},
			value:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.field.GetFieldValue(tt.value)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestOperation_BuildWhereClause(t *testing.T) {
	tests := []struct {
		name       string
		operation  Operation
		conditions map[string]interface{}
		wantSQL    string
		wantValues []interface{}
		wantErr    bool
	}{
		{
			name: "simple where clause",
			operation: Operation{
				WhereConditions: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
				},
			},
			conditions: map[string]interface{}{
				"note_id": "123e4567-e89b-12d3-a456-426614174000",
			},
			wantSQL:    "note_id = ?",
			wantValues: []interface{}{"123e4567-e89b-12d3-a456-426614174000"},
			wantErr:    false,
		},
		{
			name: "multiple where conditions",
			operation: Operation{
				WhereConditions: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
					"user_id": {
						Type:     "int64",
						Required: false,
					},
				},
			},
			conditions: map[string]interface{}{
				"note_id": "123e4567-e89b-12d3-a456-426614174000",
				"user_id": int64(123),
			},
			wantSQL:    "note_id = ? AND user_id = ?",
			wantValues: []interface{}{"123e4567-e89b-12d3-a456-426614174000", int64(123)},
			wantErr:    false,
		},
		{
			name: "missing required field",
			operation: Operation{
				WhereConditions: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
				},
			},
			conditions: map[string]interface{}{},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, values, err := tt.operation.BuildWhereClause(tt.conditions)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantValues, values)
		})
	}
}

func TestRequestConfig_GetRequestHandler(t *testing.T) {
	tests := []struct {
		name    string
		config  RequestConfig
		wantErr bool
	}{
		{
			name: "valid rabbitmq config",
			config: RequestConfig{
				From: "rabbitmq",
				Config: map[string]interface{}{
					"queue":       "test_queue",
					"routing_key": "test_key",
					"message": map[string]interface{}{
						"operation": "create",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid http config",
			config: RequestConfig{
				From: "http",
				Config: map[string]interface{}{
					"url": "https://api.example.com/notes",
					"body": map[string]interface{}{
						"operation": "create",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid request type",
			config: RequestConfig{
				From:   "invalid",
				Config: map[string]interface{}{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := tt.config.GetRequestHandler()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, handler)
			require.Equal(t, tt.config.From, handler.GetType())
		})
	}
}

func TestRabbitMQRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request RabbitMQRequest
		wantErr bool
	}{
		{
			name: "valid rabbitmq request",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: false,
		},
		{
			name: "missing queue",
			request: RabbitMQRequest{
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: true,
		},
		{
			name: "missing routing key",
			request: RabbitMQRequest{
				Queue: "test_queue",
				Message: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: true,
		},
		{
			name: "missing message",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHTTPRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request HTTPRequest
		wantErr bool
	}{
		{
			name: "valid http request",
			request: HTTPRequest{
				URL: "https://api.example.com/notes",
				Body: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: false,
		},
		{
			name: "missing url",
			request: HTTPRequest{
				Body: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: true,
		},
		{
			name: "empty url",
			request: HTTPRequest{
				URL: "",
				Body: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
