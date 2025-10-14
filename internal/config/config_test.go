package config

import (
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

			err = cfg.Validate()
			tt.wantErr(t, err)

			if tt.positive {
				require.Equal(t, tt.want, cfg)
			}
		})
	}
}
