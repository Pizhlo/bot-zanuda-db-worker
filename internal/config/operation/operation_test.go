package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestLoadOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    OperationConfig
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid operations",
			path: "./testdata/valid_operations.yaml",
			want: OperationConfig{
				Operations: []Operation{
					{
						Name:    "create_notes",
						Timeout: 10000,
						Type:    OperationTypeCreate,
						Buffer:  10,
						Storages: []StorageCfg{
							{
								Name:  "postgres_notes",
								Table: "notes.notes",
							},
						},
						Fields: []Field{
							{
								Name:     "user_id",
								Type:     FieldTypeInt64,
								Required: true,
							},
							{
								Name:     "text",
								Type:     FieldTypeString,
								Required: true,
							},
						},
						Request: Request{
							From: "rabbit_notes_create",
						},
						FieldsMap: map[string]Field{
							"user_id": {
								Name:     "user_id",
								Type:     FieldTypeInt64,
								Required: true,
							},
							"text": {
								Name:     "text",
								Type:     FieldTypeString,
								Required: true,
							},
						},
						WhereFieldsMap:  make(map[string]WhereField),
						UpdateFieldsMap: make(map[string]Field),
						Hash:            []byte{129, 131, 190, 21, 247, 183, 19, 27, 10, 245, 29, 121, 179, 112, 98, 254, 53, 243, 77, 125, 104, 176, 17, 106, 34, 39, 18, 145, 232, 212, 159, 59},
					},
					{
						Name:    "update_users",
						Type:    OperationTypeUpdate,
						Timeout: 500,
						Buffer:  10,
						Storages: []StorageCfg{
							{
								Name:  "postgres_users",
								Table: "users.users",
							},
						},
						Fields: []Field{
							{
								Name:     "user_id",
								Type:     FieldTypeInt64,
								Required: true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeMin,
										Value: 10000,
									},
								},
								Validation: AggregatedValidation{
									Min: fromValToPointer(t, 10000),
								},
							},
							{
								Name:     "name",
								Type:     FieldTypeString,
								Required: true,
								ValidationsList: []Validation{
									{
										Type: ValidationTypeNotEmpty,
									},
								},
								Validation: AggregatedValidation{
									NotEmpty: true,
								},
							},
							{
								Name:     "email",
								Type:     FieldTypeString,
								Required: true,
								Update:   true,
								ValidationsList: []Validation{
									{
										Type: ValidationTypeNotEmpty,
									},
								},
								Validation: AggregatedValidation{
									NotEmpty: true,
								},
							},
							{
								Name:     "age",
								Type:     FieldTypeInt64,
								Required: true,
								Update:   true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeMin,
										Value: 18,
									},
									{
										Type:  ValidationTypeMax,
										Value: 100,
									},
								},
								Validation: AggregatedValidation{
									Min: fromValToPointer(t, 18),
									Max: fromValToPointer(t, 100),
								},
							},
							{
								Name:     "is_active",
								Type:     FieldTypeBool,
								Required: true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeExpectedValue,
										Value: true,
									},
								},
								Validation: AggregatedValidation{
									ExpectedValue: true,
								},
							},
						},
						Hash: []byte{222, 14, 225, 14, 69, 26, 240, 117, 193, 142, 175, 187, 165, 178, 178, 145, 63, 234, 53, 155, 175, 115, 248, 216, 12, 192, 63, 232, 104, 236, 121, 59},
						Request: Request{
							From: "rabbit_users_update",
						},
						FieldsMap: map[string]Field{
							"user_id": {
								Name:     "user_id",
								Type:     FieldTypeInt64,
								Required: true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeMin,
										Value: 10000,
									},
								},
								Validation: AggregatedValidation{
									Min: fromValToPointer(t, 10000),
								},
							},
							"name": {
								Name:     "name",
								Type:     FieldTypeString,
								Required: true,
								ValidationsList: []Validation{
									{
										Type: ValidationTypeNotEmpty,
									},
								},
								Validation: AggregatedValidation{
									NotEmpty: true,
								},
							},
							"email": {
								Name:     "email",
								Type:     FieldTypeString,
								Required: true,
								ValidationsList: []Validation{
									{
										Type: ValidationTypeNotEmpty,
									},
								},
								Validation: AggregatedValidation{
									NotEmpty: true,
								},
								Update: true,
							},
							"age": {
								Name:     "age",
								Type:     FieldTypeInt64,
								Required: true,
								Update:   true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeMin,
										Value: 18,
									},
									{
										Type:  ValidationTypeMax,
										Value: 100,
									},
								},
								Validation: AggregatedValidation{
									Min: fromValToPointer(t, 18),
									Max: fromValToPointer(t, 100),
								},
							},
							"is_active": {
								Name:     "is_active",
								Type:     FieldTypeBool,
								Required: true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeExpectedValue,
										Value: true,
									},
								},
								Validation: AggregatedValidation{
									ExpectedValue: true,
								},
							},
						},
						WhereFieldsMap: map[string]WhereField{
							"user_id": {
								Field:    Field{Name: "user_id", Type: FieldTypeInt64, Required: false},
								Operator: OperatorEqual,
								Value:    interface{}(nil),
							},
							"name": {
								Field:    Field{Name: "name", Type: FieldTypeString, Required: false},
								Operator: OperatorEqual,
								Value:    interface{}(nil),
							},
							"is_active": {
								Field:    Field{Name: "is_active", Type: FieldTypeBool, Required: false},
								Operator: OperatorEqual,
								Value:    true,
							},
						},
						UpdateFieldsMap: map[string]Field{
							"email": {
								Name:     "email",
								Type:     FieldTypeString,
								Required: true,
								ValidationsList: []Validation{
									{
										Type: ValidationTypeNotEmpty,
									},
								},
								Validation: AggregatedValidation{
									NotEmpty: true,
								},
								Update: true,
							},
							"age": {
								Name:     "age",
								Type:     FieldTypeInt64,
								Required: true,
								ValidationsList: []Validation{
									{
										Type:  ValidationTypeMin,
										Value: 18,
									},
									{
										Type:  ValidationTypeMax,
										Value: 100,
									},
								},
								Validation: AggregatedValidation{
									Min: fromValToPointer(t, 18),
									Max: fromValToPointer(t, 100),
								},
								Update: true,
							},
						},
						Where: []Where{
							{
								Type:   "and",
								Fields: []WhereField(nil),
								Conditions: []Where{
									{
										Type: "and",
										Fields: []WhereField{
											{Field: Field{Name: "user_id", Type: FieldTypeInt64, Required: false}, Operator: OperatorEqual, Value: interface{}(nil)},
											{Field: Field{Name: "name", Type: FieldTypeString, Required: false}, Operator: OperatorEqual, Value: interface{}(nil)},
										},
									},
									{
										Fields: []WhereField{
											{Field: Field{Name: "is_active", Type: FieldTypeBool, Required: false}, Operator: OperatorEqual, Value: true},
										},
									},
								},
							},
						},
					},
				},
				Connections: []Connection{
					{
						Name:          "rabbit_notes_create",
						Type:          ConnectionTypeRabbitMQ,
						Address:       "amqp://<user>:<password>@localhost:1234/",
						Queue:         "notes",
						RoutingKey:    "create",
						InsertTimeout: 1,
						ReadTimeout:   1,
					},
					{
						Name:          "rabbit_users_update",
						Type:          ConnectionTypeRabbitMQ,
						Address:       "amqp://<user>:<password>@localhost:1234/",
						Queue:         "users_update_queue",
						RoutingKey:    "update",
						InsertTimeout: 1,
						ReadTimeout:   1,
					},
				},
				Storages: []StorageCfg{
					{
						Name:          "postgres_notes",
						Type:          StorageTypePostgres,
						Host:          "localhost",
						Port:          5432,
						User:          "user",
						Password:      "password",
						DBName:        "test1",
						InsertTimeout: 5000000,
						ReadTimeout:   5000000,
					},
					{
						Name:          "postgres_users",
						Type:          StorageTypePostgres,
						Host:          "10.8.0.1",
						Port:          5435,
						User:          "user",
						Password:      "password",
						DBName:        "test2",
						InsertTimeout: 5000000,
						ReadTimeout:   5000000,
					},
				},
				StoragesMap: map[string]StorageCfg{
					"postgres_notes": {
						Name:          "postgres_notes",
						Type:          StorageTypePostgres,
						Host:          "localhost",
						Port:          5432,
						User:          "user",
						Password:      "password",
						DBName:        "test1",
						InsertTimeout: 5000000,
						ReadTimeout:   5000000,
					},
					"postgres_users": {
						Name:          "postgres_users",
						Type:          StorageTypePostgres,
						Host:          "10.8.0.1",
						Port:          5435,
						User:          "user",
						Password:      "password",
						DBName:        "test2",
						InsertTimeout: 5000000,
						ReadTimeout:   5000000,
					},
				},
				ConnectionsMap: map[string]Connection{
					"rabbit_notes_create": {
						Name:          "rabbit_notes_create",
						Type:          ConnectionTypeRabbitMQ,
						Address:       "amqp://<user>:<password>@localhost:1234/",
						Queue:         "notes",
						RoutingKey:    "create",
						InsertTimeout: 1,
						ReadTimeout:   1,
					},
					"rabbit_users_update": {
						Name:          "rabbit_users_update",
						Type:          ConnectionTypeRabbitMQ,
						Address:       "amqp://<user>:<password>@localhost:1234/",
						Queue:         "users_update_queue",
						RoutingKey:    "update",
						InsertTimeout: 1,
						ReadTimeout:   1,
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name:    "invalid operations",
			path:    "./testdata/invalid_operations.yaml",
			wantErr: require.Error,
		},
		{
			name:    "no update fields",
			path:    "./testdata/no_update_fields.yaml",
			wantErr: require.Error,
		},
		{
			name:    "error aggregating validation",
			path:    "./testdata/error_aggregating_validation.yaml",
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			operation, err := LoadOperation(tt.path)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, operation)
		})
	}
}

//nolint:funlen // это тест
func TestMapStorages(t *testing.T) {
	t.Parallel()

	op := OperationConfig{
		Storages: []StorageCfg{
			{
				Name:          "postgres_notes",
				Type:          StorageTypePostgres,
				Host:          "localhost",
				Port:          5432,
				User:          "user",
				Password:      "password",
				DBName:        "test",
				InsertTimeout: 5000000,
				ReadTimeout:   5000000,
			},
			{
				Name:          "postgres_reminders",
				Type:          StorageTypePostgres,
				Host:          "localhost",
				Port:          5432,
				User:          "user",
				Password:      "password",
				DBName:        "test",
				InsertTimeout: 5000000,
				ReadTimeout:   5000000,
			},
			{
				Name:          "postgres_users",
				Type:          StorageTypePostgres,
				Host:          "localhost",
				Port:          5432,
				User:          "user",
				Password:      "password",
				DBName:        "test",
				InsertTimeout: 5000000,
				ReadTimeout:   5000000,
			},
		},
	}

	expected := map[string]StorageCfg{
		"postgres_notes": {
			Name:          "postgres_notes",
			Type:          StorageTypePostgres,
			Host:          "localhost",
			Port:          5432,
			User:          "user",
			Password:      "password",
			DBName:        "test",
			InsertTimeout: 5000000,
			ReadTimeout:   5000000,
		},
		"postgres_reminders": {
			Name:          "postgres_reminders",
			Type:          StorageTypePostgres,
			Host:          "localhost",
			Port:          5432,
			User:          "user",
			Password:      "password",
			DBName:        "test",
			InsertTimeout: 5000000,
			ReadTimeout:   5000000,
		},
		"postgres_users": {
			Name:          "postgres_users",
			Type:          StorageTypePostgres,
			Host:          "localhost",
			Port:          5432,
			User:          "user",
			Password:      "password",
			DBName:        "test",
			InsertTimeout: 5000000,
			ReadTimeout:   5000000,
		},
	}

	op.mapStorages()

	assert.Equal(t, expected, op.StoragesMap)
}

//nolint:funlen // это тест
func TestMapConnections(t *testing.T) {
	t.Parallel()

	op := OperationConfig{
		Connections: []Connection{
			{
				Name:          "rabbit_notes_create",
				Type:          ConnectionTypeRabbitMQ,
				Address:       "amqp://user:password@localhost:1234/",
				Queue:         "notes",
				RoutingKey:    "create",
				InsertTimeout: 5000000,
				ReadTimeout:   5000000,
			},
			{
				Name:          "rabbit_notes_update",
				Type:          ConnectionTypeRabbitMQ,
				Address:       "amqp://user:password@localhost:1234/",
				Queue:         "notes",
				RoutingKey:    "update",
				InsertTimeout: 5000000,
				ReadTimeout:   5000000,
			},
			{
				Name:          "rabbit_notes_delete",
				Type:          ConnectionTypeRabbitMQ,
				Address:       "amqp://user:password@localhost:1234/",
				Queue:         "notes",
				RoutingKey:    "delete",
				InsertTimeout: 5000000,
				ReadTimeout:   5000000,
			},
		},
	}

	expected := map[string]Connection{
		"rabbit_notes_create": {
			Name:          "rabbit_notes_create",
			Type:          ConnectionTypeRabbitMQ,
			Address:       "amqp://user:password@localhost:1234/",
			Queue:         "notes",
			RoutingKey:    "create",
			InsertTimeout: 5000000,
			ReadTimeout:   5000000,
		},
		"rabbit_notes_update": {
			Name:          "rabbit_notes_update",
			Type:          ConnectionTypeRabbitMQ,
			Address:       "amqp://user:password@localhost:1234/",
			Queue:         "notes",
			RoutingKey:    "update",
			InsertTimeout: 5000000,
			ReadTimeout:   5000000,
		},
		"rabbit_notes_delete": {
			Name:          "rabbit_notes_delete",
			Type:          ConnectionTypeRabbitMQ,
			Address:       "amqp://user:password@localhost:1234/",
			Queue:         "notes",
			RoutingKey:    "delete",
			InsertTimeout: 5000000,
			ReadTimeout:   5000000,
		},
	}

	op.mapConnections()

	assert.Equal(t, expected, op.ConnectionsMap)
}

//nolint:funlen,dupl // это тест; проверяем одни случаи в разных тестах
func TestAggregateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		opName   string
		field    Field
		expected Field
		wantErr  require.ErrorAssertionFunc
	}{
		{
			name: "positive case #1",
			field: Field{
				Name: "field2",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeExpectedValue,
						Value: 123,
					},
					{
						Type:  ValidationTypeMin,
						Value: 10,
					},
					{
						Type:  ValidationTypeMax,
						Value: 100,
					},
				},
			},
			expected: Field{
				Name: "field2",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeExpectedValue,
						Value: 123,
					},
					{
						Type:  ValidationTypeMin,
						Value: 10,
					},
					{
						Type:  ValidationTypeMax,
						Value: 100,
					},
				},
				Validation: AggregatedValidation{
					ExpectedValue: 123,
					Min:           fromValToPointer(t, 10),
					Max:           fromValToPointer(t, 100),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case #2",
			field: Field{
				Name: "field2",
				Type: FieldTypeString,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeExpectedValue,
						Value: "string",
					},
					{
						Type:  ValidationTypeMinLength,
						Value: 10,
					},
					{
						Type:  ValidationTypeMaxLength,
						Value: 100,
					},
				},
			},
			expected: Field{
				Name: "field2",
				Type: FieldTypeString,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeExpectedValue,
						Value: "string",
					},
					{
						Type:  ValidationTypeMinLength,
						Value: 10,
					},
					{
						Type:  ValidationTypeMaxLength,
						Value: 100,
					},
				},
				Validation: AggregatedValidation{
					ExpectedValue: "string",
					MinLength:     fromValToPointer(t, 10),
					MaxLength:     fromValToPointer(t, 100),
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: validation type min: value is not int",
			field: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMin,
						Value: "string",
					},
				},
			},
			expected: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMin,
						Value: "string",
					},
				},
			},
			opName:  "create_notes",
			wantErr: require.Error,
		},
		{
			name: "negative case: validation type max: value is not int",
			field: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMax,
						Value: "string",
					},
				},
			},
			expected: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMax,
						Value: "string",
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: validation type max_length: value is not int",
			field: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMaxLength,
						Value: "string",
					},
				},
			},
			expected: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMaxLength,
						Value: "string",
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: validation type min_length: value is not int",
			field: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMinLength,
						Value: "string",
					},
				},
			},
			expected: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMinLength,
						Value: "string",
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: validation type not_empty",
			field: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeNotEmpty,
						Value: true,
					},
				},
			},
			expected: Field{
				Name: "field1",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeNotEmpty,
						Value: true,
					},
				},
				Validation: AggregatedValidation{
					NotEmpty: true,
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := aggregateValidation(test.opName, test.field)
			test.wantErr(t, err)

			require.Equal(t, test.expected, got)
		})
	}
}

//nolint:funlen // это тест
func TestMapFieldsByOperation(t *testing.T) {
	t.Parallel()

	op := Operation{
		Name: "test",
		Fields: []Field{
			{
				Name: "field1",
				Type: FieldTypeString,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMinLength,
						Value: 10,
					},
				},
				Validation: AggregatedValidation{
					MinLength: fromValToPointer(t, 10),
				},
				Required: true,
			},
			{
				Name: "field2",
				Type: FieldTypeInt64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeMax,
						Value: 123,
					},
				},
				Validation: AggregatedValidation{
					Max: fromValToPointer(t, 123),
				},
				Required: true,
			},
			{
				Name: "field3",
				Type: FieldTypeFloat64,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeExpectedValue,
						Value: 123.45,
					},
				},
				Validation: AggregatedValidation{
					ExpectedValue: 123.45,
				},
				Required: true,
			},
			{
				Name: "field4",
				Type: FieldTypeBool,
				ValidationsList: []Validation{
					{
						Type:  ValidationTypeExpectedValue,
						Value: true,
					},
				},
				Validation: AggregatedValidation{
					ExpectedValue: true,
				},
				Required: true,
			},
		},
	}

	expected := map[string]Field{
		"field1": {
			Name: "field1",
			Type: FieldTypeString,
			ValidationsList: []Validation{
				{
					Type:  ValidationTypeMinLength,
					Value: 10,
				},
			},
			Validation: AggregatedValidation{
				MinLength: fromValToPointer(t, 10),
			},
			Required: true,
		},
		"field2": {
			Name: "field2",
			Type: FieldTypeInt64,
			ValidationsList: []Validation{
				{
					Type:  ValidationTypeMax,
					Value: 123,
				},
			},
			Validation: AggregatedValidation{
				Max: fromValToPointer(t, 123),
			},
			Required: true,
		},
		"field3": {
			Name: "field3",
			Type: FieldTypeFloat64,
			ValidationsList: []Validation{
				{
					Type:  ValidationTypeExpectedValue,
					Value: 123.45,
				},
			},
			Validation: AggregatedValidation{
				ExpectedValue: 123.45,
			},
			Required: true,
		},
		"field4": {
			Name: "field4",
			Type: FieldTypeBool,
			ValidationsList: []Validation{
				{
					Type:  ValidationTypeExpectedValue,
					Value: true,
				},
			},
			Validation: AggregatedValidation{
				ExpectedValue: true,
			},
			Required: true,
		},
	}

	op.mapFieldsByOperation()

	assert.Equal(t, expected, op.FieldsMap)
}

//nolint:funlen // это тест
func TestCalculateHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation Operation
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "successful hash calculation for create operation",
			operation: Operation{
				Name:    "create_notes",
				Timeout: 10000,
				Type:    OperationTypeCreate,
				Storages: []StorageCfg{
					{
						Name:  "postgres_notes",
						Table: "notes.notes",
					},
				},
				Fields: []Field{
					{
						Name:     "user_id",
						Type:     FieldTypeInt64,
						Required: true,
					},
					{
						Name:     "text",
						Type:     FieldTypeString,
						Required: true,
					},
				},
				Request: Request{
					From: "rabbit_notes_create",
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "successful hash calculation for update operation with where",
			operation: Operation{
				Name:    "update_users",
				Timeout: 500,
				Type:    OperationTypeUpdate,
				Storages: []StorageCfg{
					{
						Name:  "postgres_users",
						Table: "users.users",
					},
				},
				Fields: []Field{
					{
						Name:     "user_id",
						Type:     FieldTypeInt64,
						Required: true,
					},
					{
						Name:     "name",
						Type:     FieldTypeString,
						Required: true,
						Update:   true,
					},
				},
				Request: Request{
					From: "rabbit_users_update",
				},
				Where: []Where{
					{
						Type: "and",
						Fields: []WhereField{
							{
								Field: Field{
									Name: "user_id",
									Type: FieldTypeInt64,
								},
								Operator: OperatorEqual,
								Value:    123,
							},
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "successful hash calculation for delete operation",
			operation: Operation{
				Name:    "delete_notes",
				Timeout: 1000,
				Type:    OperationTypeDelete,
				Storages: []StorageCfg{
					{
						Name:  "postgres_notes",
						Table: "notes.notes",
					},
				},
				Fields: []Field{
					{
						Name:     "id",
						Type:     FieldTypeInt64,
						Required: true,
					},
				},
				Request: Request{
					From: "rabbit_notes_delete",
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.operation.calculateHash()
			tt.wantErr(t, err)

			// Проверяем, что хеш был вычислен и не пустой
			assert.NotEmpty(t, tt.operation.Hash, "Hash should not be empty")
			assert.Len(t, tt.operation.Hash, 32, "Hash should be 32 bytes (SHA256)")
		})
	}
}

//nolint:dupl // похожие тесты
func TestCalculateHashDeterministic(t *testing.T) {
	t.Parallel()

	// Создаем две одинаковые операции
	operation1 := Operation{
		Name:    "test_operation",
		Timeout: 1000,
		Type:    OperationTypeCreate,
		Storages: []StorageCfg{
			{
				Name:  "postgres_test",
				Table: "test.table",
			},
		},
		Fields: []Field{
			{
				Name:     "field1",
				Type:     FieldTypeString,
				Required: true,
			},
		},
		Request: Request{
			From: "rabbit_test",
		},
	}

	operation2 := Operation{
		Name:    "test_operation",
		Timeout: 1000,
		Type:    OperationTypeCreate,
		Storages: []StorageCfg{
			{
				Name:  "postgres_test",
				Table: "test.table",
			},
		},
		Fields: []Field{
			{
				Name:     "field1",
				Type:     FieldTypeString,
				Required: true,
			},
		},
		Request: Request{
			From: "rabbit_test",
		},
	}

	// Вычисляем хеши
	err1 := operation1.calculateHash()
	require.NoError(t, err1)

	err2 := operation2.calculateHash()
	require.NoError(t, err2)

	// Проверяем, что хеши одинаковые
	assert.Equal(t, operation1.Hash, operation2.Hash, "Identical operations should produce identical hashes")
}

//nolint:dupl // похожие тесты
func TestCalculateHashDifferentOperations(t *testing.T) {
	t.Parallel()

	// Создаем две разные операции
	operation1 := Operation{
		Name:    "operation1",
		Timeout: 1000,
		Type:    OperationTypeCreate,
		Storages: []StorageCfg{
			{
				Name:  "postgres_test",
				Table: "test.table",
			},
		},
		Fields: []Field{
			{
				Name:     "field1",
				Type:     FieldTypeString,
				Required: true,
			},
		},
		Request: Request{
			From: "rabbit_test",
		},
	}

	operation2 := Operation{
		Name:    "operation2", // Разное имя
		Timeout: 1000,
		Type:    OperationTypeCreate,
		Storages: []StorageCfg{
			{
				Name:  "postgres_test",
				Table: "test.table",
			},
		},
		Fields: []Field{
			{
				Name:     "field1",
				Type:     FieldTypeString,
				Required: true,
			},
		},
		Request: Request{
			From: "rabbit_test",
		},
	}

	// Вычисляем хеши
	err1 := operation1.calculateHash()
	require.NoError(t, err1)

	err2 := operation2.calculateHash()
	require.NoError(t, err2)

	// Проверяем, что хеши разные
	assert.NotEqual(t, operation1.Hash, operation2.Hash, "Different operations should produce different hashes")
}

//nolint:funlen // длинный тест
func TestCalculateHashFieldSensitivity(t *testing.T) {
	t.Parallel()

	// Создаем базовую операцию
	baseOperation := Operation{
		Name:    "test_operation",
		Timeout: 1000,
		Type:    OperationTypeCreate,
		Storages: []StorageCfg{
			{
				Name:  "postgres_test",
				Table: "test.table",
			},
		},
		Fields: []Field{
			{
				Name:     "field1",
				Type:     FieldTypeString,
				Required: true,
			},
		},
		Request: Request{
			From: "rabbit_test",
		},
	}

	// Вычисляем хеш базовой операции
	err := baseOperation.calculateHash()
	require.NoError(t, err)

	baseHash := make([]byte, len(baseOperation.Hash))
	copy(baseHash, baseOperation.Hash)

	// Тестируем чувствительность к изменению различных полей
	testCases := []struct {
		name     string
		modifier func(*Operation)
	}{
		{
			name: "name change",
			modifier: func(op *Operation) {
				op.Name = "different_name"
			},
		},
		{
			name: "timeout change",
			modifier: func(op *Operation) {
				op.Timeout = 2000
			},
		},
		{
			name: "type change",
			modifier: func(op *Operation) {
				op.Type = OperationTypeUpdate
			},
		},
		{
			name: "storage change",
			modifier: func(op *Operation) {
				op.Storages[0].Table = "different.table"
			},
		},
		{
			name: "field change",
			modifier: func(op *Operation) {
				op.Fields[0].Name = "different_field"
			},
		},
		{
			name: "request change",
			modifier: func(op *Operation) {
				op.Request.From = "different_rabbit"
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Создаем копию операции
			modifiedOperation := baseOperation
			modifiedOperation.Storages = make([]StorageCfg, len(baseOperation.Storages))
			copy(modifiedOperation.Storages, baseOperation.Storages)
			modifiedOperation.Fields = make([]Field, len(baseOperation.Fields))
			copy(modifiedOperation.Fields, baseOperation.Fields)

			// Применяем модификацию
			tc.modifier(&modifiedOperation)

			// Вычисляем хеш модифицированной операции
			err := modifiedOperation.calculateHash()
			require.NoError(t, err)

			// Проверяем, что хеш изменился
			assert.NotEqual(t, baseHash, modifiedOperation.Hash, "Hash should change when %s is modified", tc.name)
		})
	}
}
