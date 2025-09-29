package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v2"
)

type RabbitMQ struct {
	Address       string `yaml:"address" validate:"required,rabbitmq_address"`
	NoteExchange  string `yaml:"note_exchange" validate:"required"`
	SpaceExchange string `yaml:"space_exchange" validate:"required"`
	InsertTimeout int    `yaml:"insert_timeout" validate:"required,min=1"`
	ReadTimeout   int    `yaml:"read_timeout" validate:"required,min=1"`
}

type Postgres struct {
	Host          string `yaml:"host" validate:"required"`
	Port          int    `yaml:"port" validate:"required,min=1024,max=65535"`
	User          string `yaml:"user" validate:"required"`
	Password      string `yaml:"password" validate:"required"`
	DBName        string `yaml:"db_name" validate:"required"`
	InsertTimeout int    `yaml:"insert_timeout" validate:"required,min=1"`
	ReadTimeout   int    `yaml:"read_timeout" validate:"required,min=1"`
}

type Config struct {
	LogLevel   string `yaml:"log_level" validate:"required,oneof=debug info warn error"`
	InstanceID int    `yaml:"instance_id" validate:"required,min=1"`

	Storage struct {
		BufferSize int      `yaml:"buffer_size" validate:"required,min=1"`
		Postgres   Postgres `yaml:"postgres"`
		RabbitMQ   RabbitMQ `yaml:"rabbitmq"`
	} `yaml:"storage"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}

	// Читаем YAML файл
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: error read file: %w", err)
	}

	// Парсим YAML
	if err := yaml.Unmarshal(yamlFile, cfg); err != nil {
		return nil, fmt.Errorf("config: error unmarshal: %w", err)
	}

	// Создаем валидатор
	validate := validator.New()

	err = validate.RegisterValidation("rabbitmq_address", ValidateRabbitMQAddress)
	if err != nil {
		return nil, fmt.Errorf("config: error register validation: %w", err)
	}

	// Валидируем конфиг
	if err := validate.Struct(cfg); err != nil {
		return nil, fmt.Errorf("config: error validate: %w", err)
	}

	return cfg, nil
}

// ValidateRabbitMQAddress implements validator.Func
func ValidateRabbitMQAddress(fl validator.FieldLevel) bool {
	return strings.HasPrefix(fl.Field().String(), "amqp://")
}
