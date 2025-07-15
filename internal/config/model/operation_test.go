package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildWhereClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		operation     Operation
		conditions    map[string]interface{}
		wantSQLClause SQLClause
		wantErr       bool
	}{
		{
			name: "empty where conditions",
			operation: Operation{
				WhereConditions: map[string]Field{},
			},
			conditions: map[string]interface{}{},
			wantSQLClause: SQLClause{
				SQL:  "",
				Args: nil,
			},
		},
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
			wantSQLClause: SQLClause{
				SQL:  "note_id = ?",
				Args: []interface{}{"123e4567-e89b-12d3-a456-426614174000"},
			},
			wantErr: false,
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
						Type:     fieldTypeInt64,
						Required: false,
					},
				},
			},
			conditions: map[string]interface{}{
				"note_id": "123e4567-e89b-12d3-a456-426614174000",
				"user_id": int64(123),
			},
			wantSQLClause: SQLClause{
				SQL:  "note_id = ? AND user_id = ?",
				Args: []interface{}{"123e4567-e89b-12d3-a456-426614174000", int64(123)},
			},
			wantErr: false,
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
			t.Parallel()

			sql, err := tt.operation.BuildWhereClause(tt.conditions)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.wantSQLClause, sql)
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation Operation
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "valid operation without where conditions",
			operation: Operation{
				Fields: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
					"request_id": {
						Type:     "uuid",
						Required: true,
					},
				},
				Request: &RequestConfig{
					Connection: Connection{
						Type: "rabbitmq",
					},
					Config: map[string]any{
						"address":     "amqp://user:password@localhost:5672/",
						"queue":       "notes",
						"routing_key": "notes",
						"message": map[string]interface{}{
							"operation": map[string]interface{}{
								"type":     fieldTypeString,
								"required": true,
								"value":    "create",
							},
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "valid operation with where conditions",
			operation: Operation{
				Fields: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
					"request_id": {
						Type:     "uuid",
						Required: true,
					},
				},
				WhereConditions: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
				},
				Request: &RequestConfig{
					Connection: Connection{
						Type: "rabbitmq",
					},
					Config: map[string]any{
						"address":     "amqp://user:password@localhost:5672/",
						"queue":       "notes",
						"routing_key": "notes",
						"message": map[string]interface{}{
							"operation": map[string]interface{}{
								"type":     fieldTypeString,
								"required": true,
								"value":    "create",
							},
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "without request_id field",
			operation: Operation{
				Fields: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
				},
				Request: &RequestConfig{
					Connection: Connection{
						Type: "rabbitmq",
					},
					Config: map[string]any{
						"address":     "amqp://user:password@localhost:5672/",
						"queue":       "notes",
						"routing_key": "notes",
						"message": map[string]interface{}{
							"operation": map[string]interface{}{
								"type":     fieldTypeString,
								"required": true,
								"value":    "create",
							},
						},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "request_id is not uuid",
			operation: Operation{
				Fields: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
					"request_id": {
						Type:     fieldTypeString,
						Required: true,
					},
				},
				Request: &RequestConfig{
					Connection: Connection{
						Type: "rabbitmq",
					},
					Config: map[string]any{
						"address":     "amqp://user:password@localhost:5672/",
						"queue":       "notes",
						"routing_key": "notes",
						"message": map[string]interface{}{
							"operation": map[string]interface{}{
								"type":     fieldTypeString,
								"required": true,
								"value":    "create",
							},
						},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "request_id is not required",
			operation: Operation{
				Fields: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
					"request_id": {
						Type:     "uuid",
						Required: false,
					},
				},
				Request: &RequestConfig{
					Connection: Connection{
						Type: "rabbitmq",
					},
					Config: map[string]any{
						"address":     "amqp://user:password@localhost:5672/",
						"queue":       "notes",
						"routing_key": "notes",
						"message": map[string]interface{}{
							"operation": map[string]interface{}{
								"type":     fieldTypeString,
								"required": true,
								"value":    "create",
							},
						},
					},
				},
			},
			wantErr: require.Error,
		},
		// {
		// 	name: "without request config",
		// 	operation: Operation{
		// 		Fields: map[string]Field{
		// 			"note_id": {
		// 				Type:     "uuid",
		// 				Required: true,
		// 			},
		// 			"request_id": {
		// 				Type:     "uuid",
		// 				Required: true,
		// 			},
		// 		},
		// 	},
		// 	wantErr: require.Error,
		// },
		{
			name: "unknown field in where conditions",
			operation: Operation{
				Fields: map[string]Field{
					"note_id": {
						Type:     "uuid",
						Required: true,
					},
					"request_id": {
						Type:     "uuid",
						Required: true,
					},
				},
				WhereConditions: map[string]Field{
					"some_field": {
						Type:     "uuid",
						Required: true,
					},
				},
				Request: &RequestConfig{
					Connection: Connection{
						Type: "rabbitmq",
					},
					Config: map[string]any{
						"address":     "amqp://user:password@localhost:5672/",
						"queue":       "notes",
						"routing_key": "notes",
						"message": map[string]interface{}{
							"operation": map[string]interface{}{
								"type":     fieldTypeString,
								"required": true,
								"value":    "create",
							},
						},
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.operation.Validate([]Connection{tt.operation.Request.Connection})
			tt.wantErr(t, err)
		})
	}
}
