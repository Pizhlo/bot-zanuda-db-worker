package config

import (
	"db-worker/internal/config/operation"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v2"
)

// Postgres - конфигурация PostgreSQL.
type Postgres struct {
	Host          string `yaml:"host" validate:"required"`
	Port          int    `yaml:"port" validate:"required,min=1024,max=65535"`
	User          string `yaml:"user" validate:"required"`
	Password      string `yaml:"password" validate:"required"`
	DBName        string `yaml:"db_name" validate:"required"`
	InsertTimeout int    `yaml:"insert_timeout" validate:"required,min=1"`
	ReadTimeout   int    `yaml:"read_timeout" validate:"required,min=1"`
}

// RedisType - тип подключения к Redis: single - один узел, cluster - кластер.
type RedisType string

const (
	// RedisTypeSingle - один узел.
	RedisTypeSingle RedisType = "single"
	// RedisTypeCluster - кластер.
	RedisTypeCluster RedisType = "cluster"
)

// Redis - конфигурация Redis.
type Redis struct {
	Type RedisType `yaml:"type" validate:"required,oneof=single cluster"`
	// single
	Host string `yaml:"host" validate:"omitempty,hostname"`
	Port int    `yaml:"port" validate:"omitempty,min=1024,max=65535"`
	// cluster
	Addrs []string `yaml:"addrs" validate:"omitempty,dive,hostname_port"`

	InsertTimeout int `yaml:"insert_timeout" validate:"required,min=1"`
	ReadTimeout   int `yaml:"read_timeout" validate:"required,min=1"`
}

// Config - конфигурация.
type Config struct {
	LogLevel   string `yaml:"log_level" validate:"required,oneof=debug info warn error"`
	InstanceID int    `yaml:"instance_id" validate:"required,min=1"`

	Server Server `yaml:"server" validate:"required"`

	Storage struct {
		BufferSize int      `yaml:"buffer_size" validate:"required,min=1"`
		Postgres   Postgres `yaml:"postgres"`
		Redis      Redis    `yaml:"redis" validate:"required"`
	} `yaml:"storage"`

	Operations operation.OperationConfig `validate:"-"` // валидируется в LoadOperationConfig
}

// Server - конфигурация сервера.
type Server struct {
	Port            int           `yaml:"port" validate:"required,min=1024,max=65535"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" validate:"required,min=1ms"`
}

// LoadConfig загружает конфигурацию.
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}

	// Читаем YAML файл
	yamlFile, err := os.ReadFile(path) //nolint:gosec // заведена задача BZ-17
	if err != nil {
		return nil, fmt.Errorf("config: error read file: %w", err)
	}

	// Парсим YAML
	if err := yaml.Unmarshal(yamlFile, cfg); err != nil {
		return nil, fmt.Errorf("config: error unmarshal: %w", err)
	}

	return cfg, nil
}

// LoadOperationConfig загружает конфигурацию операций.
func (cfg *Config) LoadOperationConfig(path string) error {
	operations, err := operation.LoadOperation(path)
	if err != nil {
		return fmt.Errorf("config: error loading operation config: %w", err)
	}

	cfg.Operations = operations

	return nil
}

// Validate валидирует конфиг.
func (cfg *Config) Validate() error {
	// Создаем валидатор
	validate := validator.New()

	err := validate.RegisterValidation("rabbitmq_address", ValidateRabbitMQAddress)
	if err != nil {
		return fmt.Errorf("config: error register validation: %w", err)
	}

	if err := validate.Struct(cfg); err != nil {
		return fmt.Errorf("config: error validate: %w", err)
	}

	if err := cfg.validateRedisConfig(); err != nil {
		return fmt.Errorf("config: error validate redis config: %w", err)
	}

	return nil
}

// ValidateRabbitMQAddress implements validator.Func.
func ValidateRabbitMQAddress(fl validator.FieldLevel) bool {
	return strings.HasPrefix(fl.Field().String(), "amqp://")
}

func (cfg *Config) validateRedisConfig() error {
	switch cfg.Storage.Redis.Type {
	case RedisTypeSingle:
		return validateRedisSingleConfig(&cfg.Storage.Redis)
	case RedisTypeCluster:
		return validateRedisClusterConfig(&cfg.Storage.Redis)
	}

	// нет default, т.к. валидируется в validate.Struct
	return nil
}

func validateRedisSingleConfig(cfg *Redis) error {
	if cfg.Host == "" || cfg.Port == 0 {
		return fmt.Errorf("config: host and port are required for single redis")
	}

	if len(cfg.Addrs) > 0 {
		return fmt.Errorf("config: addrs are not allowed for single redis")
	}

	return nil
}

func validateRedisClusterConfig(cfg *Redis) error {
	if len(cfg.Addrs) == 0 {
		return fmt.Errorf("config: addrs are required for cluster redis")
	}

	if cfg.Host != "" || cfg.Port != 0 {
		return fmt.Errorf("config: host and port are not allowed for cluster redis")
	}

	return nil
}
