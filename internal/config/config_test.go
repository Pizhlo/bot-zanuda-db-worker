package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		want    *Config
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid config",
			file: "testdata/valid.yaml",
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
			},
			wantErr: require.NoError,
		},
		{
			name:    "invalid config",
			file:    "testdata/invalid.yaml",
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(tt.file)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, cfg)
		})
	}
}
