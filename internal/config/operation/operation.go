package operation

import (
	"fmt"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// Type - тип операции.
type Type string

const (
	// OperationTypeCreate - создание.
	OperationTypeCreate Type = "create"
	// OperationTypeUpdate - обновление.
	OperationTypeUpdate Type = "update"
	// OperationTypeDelete - удаление.
	OperationTypeDelete Type = "delete"
	// OperationTypeDeleteAll - удаление всех.
	OperationTypeDeleteAll Type = "delete_all"
)

// OperationConfig - конфигурация операций, которые будут выполнены над моделью.
//
//nolint:revive // для различия между обычным конфигом и конфигом операций.
type OperationConfig struct {
	Operations  []Operation  `yaml:"operations" validate:"required,dive"`  // операции, которые нужно выполнить над моделью
	Connections []Connection `yaml:"connections" validate:"required,dive"` // соединения для получения сообщений
	Storages    []Storage    `yaml:"storages" validate:"required,dive"`    // куда сохранять модели

	StoragesMap    map[string]Storage
	ConnectionsMap map[string]Connection
}

// Operation - операция, которая будет выполнена над моделью.
type Operation struct {
	Name     string    `yaml:"name" validate:"required"`
	Type     Type      `yaml:"type" validate:"required,oneof=create update delete delete_all"`
	Storages []Storage `yaml:"storage" validate:"required,dive"` // куда сохранять модели. если несколько - будет сохраняться транзакцией
	Fields   []Field   `yaml:"fields" validate:"required,dive"`
	Request  Request   `yaml:"request" validate:"required"`
}

// ConnectionType - тип соединения.
type ConnectionType string

const (
	// ConnectionTypeRabbitMQ - RabbitMQ.
	ConnectionTypeRabbitMQ ConnectionType = "rabbitmq"
)

// Connection - соединение, откуда будет получен запрос на операцию.
type Connection struct {
	Name          string         `yaml:"name" validate:"required"`
	Type          ConnectionType `yaml:"type" validate:"required,oneof=rabbitmq"`
	Address       string         `yaml:"address" validate:"required"`
	Queue         string         `yaml:"queue" validate:"required"`
	RoutingKey    string         `yaml:"routing_key" validate:"required"`
	InsertTimeout int            `yaml:"insert_timeout" validate:"min=1"`
	ReadTimeout   int            `yaml:"read_timeout" validate:"min=1"`
}

// StorageType - тип хранилища.
type StorageType string

const (
	// StorageTypePostgres - PostgreSQL.
	StorageTypePostgres StorageType = "postgres"
	// StorageTypeRabbitMQ - RabbitMQ.
	StorageTypeRabbitMQ StorageType = "rabbitmq"
)

// Storage - хранилище, куда будут сохраняться модели.
type Storage struct {
	Name string      `yaml:"name"`
	Type StorageType `yaml:"type"`

	// postgres
	Table    string `yaml:"table"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"db_name"`

	// rabbitmq
	Queue      string `yaml:"queue"`
	RoutingKey string `yaml:"routing_key"`

	// timeout
	InsertTimeout int `yaml:"insert_timeout"`
	ReadTimeout   int `yaml:"read_timeout" `
}

// FieldType - тип поля сообщения.
type FieldType string

const (
	// FieldTypeString - строка.
	FieldTypeString FieldType = "string"
	// FieldTypeInt64 - целое число.
	FieldTypeInt64 FieldType = "int64"
	// FieldTypeFloat64 - число с плавающей точкой.
	FieldTypeFloat64 FieldType = "float64"
	// FieldTypeBool - логическое значение.
	FieldTypeBool FieldType = "bool"
	// FieldTypeUUID - UUID.
	FieldTypeUUID FieldType = "uuid"
)

// Field - поле сообщения.
type Field struct {
	Name     string    `yaml:"name"`
	Type     FieldType `yaml:"type" validate:"required,oneof=string int64 float64 bool uuid"`
	Required bool      `yaml:"required"`
}

// Request - откуда будет получен запрос на операцию.
type Request struct {
	From string `yaml:"from"`
}

// LoadOperation загружает конфигурацию операции.
func LoadOperation(path string) (OperationConfig, error) {
	yamlFile, err := os.ReadFile(path) //nolint:gosec // заведена задача BZ-17
	if err != nil {
		return OperationConfig{}, fmt.Errorf("error reading file: %w", err)
	}

	var operationConfig OperationConfig

	err = yaml.Unmarshal(yamlFile, &operationConfig)
	if err != nil {
		return OperationConfig{}, fmt.Errorf("error unmarshalling file: %w", err)
	}

	err = validator.New().Struct(operationConfig)
	if err != nil {
		return OperationConfig{}, fmt.Errorf("error validating operation config: %w", err)
	}

	operationConfig.mapStorages()
	operationConfig.mapConnections()

	logrus.Infof("loaded %d operation(s)", len(operationConfig.Operations))
	logrus.Infof("loaded %d connection(s)", len(operationConfig.Connections))
	logrus.Infof("loaded %d storage(s)", len(operationConfig.Storages))

	return operationConfig, nil
}

func (oc *OperationConfig) mapStorages() {
	oc.StoragesMap = make(map[string]Storage)

	for _, storage := range oc.Storages {
		oc.StoragesMap[storage.Name] = storage
	}
}

func (oc *OperationConfig) mapConnections() {
	oc.ConnectionsMap = make(map[string]Connection)

	for _, connection := range oc.Connections {
		oc.ConnectionsMap[connection.Name] = connection
	}
}
