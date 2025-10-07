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
						Storages: []Storage{
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
				},
				Connections: []Connection{
					{
						Name:          "rabbit_notes_create",
						Type:          ConnectionTypeRabbitMQ,
						Address:       "amqp://user:password@localhost:1234/",
						Queue:         "notes",
						RoutingKey:    "create",
						InsertTimeout: 1,
						ReadTimeout:   1,
					},
				},
				Storages: []Storage{
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
				},
				StoragesMap: map[string]Storage{
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
				},
				ConnectionsMap: map[string]Connection{
					"rabbit_notes_create": {
						Name:          "rabbit_notes_create",
						Type:          ConnectionTypeRabbitMQ,
						Address:       "amqp://user:password@localhost:1234/",
						Queue:         "notes",
						RoutingKey:    "create",
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
		Storages: []Storage{
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

	expected := map[string]Storage{
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
