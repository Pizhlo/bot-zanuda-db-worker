package config

import (
	"db-worker/internal/config/operation"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		configFile     string
		operationsFile string
		want           *Config
		positive       bool
		wantErr        require.ErrorAssertionFunc
		operationsErr  require.ErrorAssertionFunc
	}{
		{
			name:           "valid config",
			configFile:     "testdata/valid.yaml",
			operationsFile: "./operation/testdata/valid_operations.yaml",
			positive:       true,
			want: &Config{
				LogLevel:   "debug",
				InstanceID: 1,
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
				}{
					BufferSize: 100,
					Postgres: Postgres{
						Host:          "localhost",
						Port:          1111,
						User:          "user",
						Password:      "password",
						DBName:        "test",
						InsertTimeout: 5000000,
						ReadTimeout:   5000000,
					},
				},
				Operations: operation.OperationConfig{
					Operations: []operation.Operation{
						{
							Name: "create_notes",
							Type: operation.OperationTypeCreate,
							Storages: []operation.Storage{
								{
									Name:  "postgres_notes",
									Table: "notes.notes",
								},
							},
							Fields: []operation.Field{
								{
									Name:     "user_id",
									Type:     operation.FieldTypeInt64,
									Required: true,
								},
								{
									Name:     "text",
									Type:     operation.FieldTypeString,
									Required: true,
								},
							},
							Request: operation.Request{
								From: "rabbit_notes_create",
							},
						},
					},
					Connections: []operation.Connection{
						{
							Name:          "rabbit_notes_create",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://user:password@localhost:1234/",
							Queue:         "notes",
							RoutingKey:    "create",
							InsertTimeout: 1,
							ReadTimeout:   1,
						},
					},
					Storages: []operation.Storage{
						{
							Name:          "postgres_notes",
							Type:          operation.StorageTypePostgres,
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test",
							InsertTimeout: 5000000,
							ReadTimeout:   5000000,
						},
					},
					StoragesMap: map[string]operation.Storage{
						"postgres_notes": {
							Name:          "postgres_notes",
							Type:          operation.StorageTypePostgres,
							Host:          "localhost",
							Port:          5432,
							User:          "user",
							Password:      "password",
							DBName:        "test",
							InsertTimeout: 5000000,
							ReadTimeout:   5000000,
						},
					},
					ConnectionsMap: map[string]operation.Connection{
						"rabbit_notes_create": {
							Name:          "rabbit_notes_create",
							Type:          operation.ConnectionTypeRabbitMQ,
							Address:       "amqp://user:password@localhost:1234/",
							Queue:         "notes",
							RoutingKey:    "create",
							InsertTimeout: 1,
							ReadTimeout:   1,
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name:           "invalid config",
			configFile:     "testdata/invalid.yaml",
			operationsFile: "./operation/testdata/valid_operations.yaml",
			positive:       false,
			wantErr:        require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := LoadConfig(tt.configFile)
			require.NoError(t, err)

			err = cfg.LoadOperationConfig(tt.operationsFile)
			require.NoError(t, err)

			err = cfg.Validate()
			tt.wantErr(t, err)

			if tt.positive {
				require.Equal(t, tt.want, cfg)
			}
		})
	}
}
