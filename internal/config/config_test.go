package config

import (
	"testing"
	"time"

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
				Server: Server{
					Port:            8080,
					ShutdownTimeout: 100 * time.Millisecond,
				},
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    "yaml:\"redis\" validate:\"required\""
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
					Redis: Redis{
						Type:          RedisTypeSingle,
						Host:          "localhost",
						Port:          6379,
						InsertTimeout: 50,
						ReadTimeout:   50,
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

//nolint:funlen // это тест
func TestValidateRedisConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *Config
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid config: single node",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type: RedisTypeSingle,
						Host: "localhost",
						Port: 6379,
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "valid config: cluster node",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type:  RedisTypeCluster,
						Addrs: []string{"localhost:6379"},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "invalid config: single node with addrs",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type:  RedisTypeSingle,
						Host:  "localhost",
						Port:  6379,
						Addrs: []string{"localhost:6379"},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid config: single node without host and port",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type: RedisTypeSingle,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid config: cluster node with host and port",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type:  RedisTypeCluster,
						Host:  "localhost",
						Port:  6379,
						Addrs: []string{"localhost:6379"},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid config: cluster node without addrs",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type: RedisTypeCluster,
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.validateRedisConfig()
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateRedisSingleConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *Config
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid config",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type: RedisTypeSingle,
						Host: "localhost",
						Port: 6379,
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "invalid config: single node with addrs",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type:  RedisTypeSingle,
						Host:  "localhost",
						Port:  6379,
						Addrs: []string{"localhost:6379"},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid config: single node without host and port",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type: RedisTypeSingle,
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateRedisSingleConfig(&tt.cfg.Storage.Redis)
			tt.wantErr(t, err)
		})
	}
}

//nolint:funlen // это тест
func TestValidateRedisClusterConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *Config
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid config",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type:  RedisTypeCluster,
						Addrs: []string{"localhost:6379"},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "invalid config: cluster node with host and port",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type:  RedisTypeCluster,
						Host:  "localhost",
						Port:  6379,
						Addrs: []string{"localhost:6379"},
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid config: cluster node without addrs",
			cfg: &Config{
				Storage: struct {
					BufferSize int      "yaml:\"buffer_size\" validate:\"required,min=1\""
					Postgres   Postgres "yaml:\"postgres\""
					Redis      Redis    `yaml:"redis" validate:"required"`
				}{
					Redis: Redis{
						Type: RedisTypeCluster,
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateRedisClusterConfig(&tt.cfg.Storage.Redis)
			tt.wantErr(t, err)
		})
	}
}
