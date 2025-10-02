package config

import (
	"db-worker/internal/config/operation"
	"fmt"
	"os"
	"strings"

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

// Config - конфигурация.
type Config struct {
	LogLevel   string `yaml:"log_level" validate:"required,oneof=debug info warn error"`
	InstanceID int    `yaml:"instance_id" validate:"required,min=1"`

	Storage struct {
		BufferSize int      `yaml:"buffer_size" validate:"required,min=1"`
		Postgres   Postgres `yaml:"postgres"`
	} `yaml:"storage"`

	Operations operation.OperationConfig
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

	return validate.Struct(cfg)
}

// ValidateRabbitMQAddress implements validator.Func.
func ValidateRabbitMQAddress(fl validator.FieldLevel) bool {
	return strings.HasPrefix(fl.Field().String(), "amqp://")
}
