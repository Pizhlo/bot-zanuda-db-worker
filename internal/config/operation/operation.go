package operation

import (
	"crypto/sha256"
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
	Storages    []StorageCfg `yaml:"storages" validate:"required,dive"`    // куда сохранять модели

	StoragesMap    map[string]StorageCfg
	ConnectionsMap map[string]Connection
}

// Operation - операция, которая будет выполнена над моделью.
type Operation struct {
	Name     string       `yaml:"name" validate:"required"`
	Timeout  int          `yaml:"timeout" validate:"required,min=1"` // время ожидания операции в миллисекундах
	Type     Type         `yaml:"type" validate:"required,oneof=create update delete"`
	Storages []StorageCfg `yaml:"storage" validate:"required,dive"` // куда сохранять модели. если несколько - будет сохраняться транзакцией
	Fields   []Field      `yaml:"fields" validate:"required,dive"`
	Request  Request      `yaml:"request" validate:"required"`
	Where    []Where      `yaml:"where" validate:"omitempty"` // условие, по которому будет выполнена операция. Только для операций update и delete

	FieldsMap       map[string]Field      `yaml:"-" validate:"-"`
	WhereFieldsMap  map[string]WhereField `yaml:"-" validate:"-"`
	UpdateFieldsMap map[string]Field      `yaml:"-" validate:"-"` // поля, которые будут обновляться (при update операции)

	Hash []byte `yaml:"-" validate:"-"` // поле для вычисления хеша операции (для версионирования)
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

// StorageCfg - хранилище, куда будут сохраняться модели.
type StorageCfg struct {
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
	Name            string               `yaml:"name"`
	Type            FieldType            `yaml:"type" validate:"required,oneof=string int64 float64 bool uuid"`
	Required        bool                 `yaml:"required"`
	ValidationsList []Validation         `yaml:"validation" validate:"omitempty,dive"`
	Validation      AggregatedValidation `yaml:"-" validate:"-"` // все валидации, которые будут применены к полю
	Update          bool                 `yaml:"update"`         // будет ли поле обновляться (при update операции)
}

// AggregatedValidation - все валидации, которые будут применены к полю.
type AggregatedValidation struct {
	Max           *int // максимальное значение
	Min           *int // минимальное значение
	MaxLength     *int // максимальная длина
	MinLength     *int // минимальная длина
	NotEmpty      bool // не пустое значение
	ExpectedValue any  // ожидаемое значение
}

// Request - откуда будет получен запрос на операцию.
type Request struct {
	From string `yaml:"from"`
}

// LoadOperation загружает конфигурацию операции.
//
//nolint:gocognit,funlen,cyclop // заведена задача BZ-36.
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

	operationConfig.mapStorages()
	operationConfig.mapConnections()

	// собираем все валидации в одну структуру для дальнейшей работы
	for i, operation := range operationConfig.Operations {
		for j, field := range operation.Fields {
			field, err = aggregateValidation(operation.Name, field)
			if err != nil {
				return OperationConfig{}, fmt.Errorf("error aggregating validation: %w", err)
			}

			operation.Fields[j] = field
		}

		operationConfig.Operations[i] = operation
	}

	logrus.WithField("count", len(operationConfig.Operations)).Info("loaded operations")
	logrus.WithField("count", len(operationConfig.Connections)).Info("loaded connections")
	logrus.WithField("count", len(operationConfig.Storages)).Info("loaded storages")

	// валидируем конфигурацию операций
	for i, operation := range operationConfig.Operations {
		for _, field := range operation.Fields {
			err = validateFieldConfig(field)
			if err != nil {
				return OperationConfig{}, fmt.Errorf("operation %q: error validating field: %w", operation.Name, err)
			}
		}

		// создаем мапу полей для быстрого доступа
		operation.mapFieldsByOperation()

		// валидируем условие where
		err = operation.validateWhereCondition()
		if err != nil {
			return OperationConfig{}, fmt.Errorf("operation %q: error validating where condition: %w", operation.Name, err)
		}

		operation.WhereFieldsMap = make(map[string]WhereField)
		operation.UpdateFieldsMap = make(map[string]Field)

		for _, where := range operation.Where {
			operation.mapWhereFields(where)
		}

		operation.mapFieldsUpdate()

		if operation.Type == OperationTypeUpdate && len(operation.Where) > 0 {
			if len(operation.UpdateFieldsMap) == 0 {
				return OperationConfig{}, fmt.Errorf("operation %q: no update fields", operation.Name)
			}
		}

		for _, where := range operation.Where {
			err = operation.validateWhereFieldUpdate(where)
			if err != nil {
				return OperationConfig{}, fmt.Errorf("operation %q: error validating update fields: %w", operation.Name, err)
			}
		}

		operationConfig.Operations[i] = operation
	}

	v := validator.New()

	err = v.Struct(operationConfig)
	if err != nil {
		return OperationConfig{}, fmt.Errorf("error validating operation config: %w", err)
	}

	for i, op := range operationConfig.Operations {
		if err := op.calculateHash(); err != nil {
			return OperationConfig{}, fmt.Errorf("operation %q: error calculating hash: %w", op.Name, err)
		}

		operationConfig.Operations[i] = op
	}

	return operationConfig, nil
}

func (oc *OperationConfig) mapStorages() {
	oc.StoragesMap = make(map[string]StorageCfg)

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

func (oc *Operation) mapFieldsByOperation() {
	oc.FieldsMap = make(map[string]Field)

	for _, field := range oc.Fields {
		oc.FieldsMap[field.Name] = field
	}
}

// aggregateValidation собирает все валидации в одну структуру для дальнейшей работы.
func aggregateValidation(opName string, field Field) (Field, error) {
	for i, validation := range field.ValidationsList {
		value := validation.Value

		switch validation.Type {
		case ValidationTypeMax:
			v, ok := value.(int)
			if !ok {
				return field, fmt.Errorf("operation %s: field %s: value is not int", opName, field.Name)
			}

			field.Validation.Max = &v
		case ValidationTypeMin:
			v, ok := value.(int)
			if !ok {
				return field, fmt.Errorf("operation %s: field %s: value is not int", opName, field.Name)
			}

			field.Validation.Min = &v
		case ValidationTypeMaxLength:
			v, ok := value.(int)
			if !ok {
				return field, fmt.Errorf("operation %s: field %s: value is not int", opName, field.Name)
			}

			field.Validation.MaxLength = &v
		case ValidationTypeMinLength:
			v, ok := value.(int)
			if !ok {
				return field, fmt.Errorf("operation %s: field %s: value is not int", opName, field.Name)
			}

			field.Validation.MinLength = &v
		case ValidationTypeNotEmpty:
			field.Validation.NotEmpty = true // если указано, то всегда true
		case ValidationTypeExpectedValue:
			field.Validation.ExpectedValue = value
		}

		field.ValidationsList[i] = validation
	}

	return field, nil
}

// calculateHash вычисляет хэщ операции (для версионирования).
func (oc *Operation) calculateHash() error {
	// operation - внутрення структура для вычисления хеша.
	type operation struct {
		Name          string       `yaml:"name" validate:"required"`
		Timeout       int          `yaml:"timeout" validate:"required,min=1"` // время ожидания операции в миллисекундах
		OperationType Type         `yaml:"type" validate:"required,oneof=create update delete delete_all"`
		Storages      []StorageCfg `yaml:"storage" validate:"required,dive"` // куда сохранять модели. если несколько - будет сохраняться транзакцией
		Fields        []Field      `yaml:"fields" validate:"required,dive"`
		Request       Request      `yaml:"request" validate:"required"`
		Where         []Where      `yaml:"where" validate:"omitempty"` // условие, по которому будет выполнена операция. Только для операций update и delete
	}

	copy := operation{
		Name:          oc.Name,
		Timeout:       oc.Timeout,
		OperationType: oc.Type,
		Storages:      oc.Storages,
		Fields:        oc.Fields,
		Request:       oc.Request,
		Where:         oc.Where,
	}

	data, err := yaml.Marshal(copy)
	if err != nil {
		return fmt.Errorf("error marshaling copy of operation to yaml: %w", err)
	}

	sum := sha256.Sum256(data)

	oc.Hash = sum[:]

	logrus.WithFields(logrus.Fields{
		"name": oc.Name,
	}).Info("successfully calculated hash of operation")

	return nil
}
