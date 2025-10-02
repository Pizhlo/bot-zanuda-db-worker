package operation

import (
	"fmt"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type OperationType string

const (
	OperationTypeCreate    OperationType = "create"
	OperationTypeUpdate    OperationType = "update"
	OperationTypeDelete    OperationType = "delete"
	OperationTypeDeleteAll OperationType = "delete_all"
)

type OperationConfig struct {
	Operations  []Operation  `yaml:"operations" validate:"required,dive"`  // операции, которые нужно выполнить над моделью
	Connections []Connection `yaml:"connections" validate:"required,dive"` // соединения для получения сообщений
	Storages    []Storage    `yaml:"storages" validate:"required,dive"`    // куда сохранять модели

	StoragesMap    map[string]Storage
	ConnectionsMap map[string]Connection
}

type Operation struct {
	Name     string        `yaml:"name" validate:"required"`
	Type     OperationType `yaml:"type" validate:"required,oneof=create update delete delete_all"`
	Storages []Storage     `yaml:"storage" validate:"required,dive"` // куда сохранять модели. если несколько - будет сохраняться транзакцией
	Fields   []Field       `yaml:"fields" validate:"required,dive"`
	Request  Request       `yaml:"request" validate:"required"`
}

type ConnectionType string

const (
	ConnectionTypeRabbitMQ ConnectionType = "rabbitmq"
)

type Connection struct {
	Name          string         `yaml:"name" validate:"required"`
	Type          ConnectionType `yaml:"type" validate:"required,oneof=rabbitmq"`
	Address       string         `yaml:"address" validate:"required"`
	Queue         string         `yaml:"queue" validate:"required"`
	RoutingKey    string         `yaml:"routing_key" validate:"required"`
	InsertTimeout int            `yaml:"insert_timeout" validate:"min=1"`
	ReadTimeout   int            `yaml:"read_timeout" validate:"min=1"`
}

type StorageType string

const (
	StorageTypePostgres StorageType = "postgres"
	StorageTypeRabbitMQ StorageType = "rabbitmq"
)

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

type FieldType string

const (
	FieldTypeString  FieldType = "string"
	FieldTypeInt64   FieldType = "int64"
	FieldTypeFloat64 FieldType = "float64"
	FieldTypeBool    FieldType = "bool"
)

type Field struct {
	Name     string    `yaml:"name"`
	Type     FieldType `yaml:"type" validate:"required,oneof=string int64 float64 bool"`
	Required bool      `yaml:"required"`
}

type Request struct {
	From string `yaml:"from"`
}

func LoadOperation(path string) (OperationConfig, error) {
	yamlFile, err := os.ReadFile(path)
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
