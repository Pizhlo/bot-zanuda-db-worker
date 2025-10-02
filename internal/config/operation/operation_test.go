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
						Name: "create_notes",
						Type: OperationTypeCreate,
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
