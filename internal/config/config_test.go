package config

import (
	"db-worker/internal/config/operation"
	"testing"

	"github.com/stretchr/testify/require"
)

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
					RabbitMQ   RabbitMQ "yaml:\"rabbitmq\""
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
					RabbitMQ: RabbitMQ{
						Address:       "amqp://user:password@localhost:1234/",
						NoteExchange:  "notes",
						SpaceExchange: "spaces",
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
							Name:       "rabbit_notes_create",
							Type:       operation.ConnectionTypeRabbitMQ,
							Address:    "amqp://user:password@localhost:1234/",
							Queue:      "notes",
							RoutingKey: "create",
						},
					},
					Storages: []operation.Storage{
						{
							Name: "postgres_notes",
							Type: operation.StorageTypePostgres,
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
