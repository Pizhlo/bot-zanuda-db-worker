package uow

import (
	"context"
	"db-worker/internal/config/operation"
	builder_pkg "db-worker/internal/service/builder"
	"db-worker/internal/storage"
	"errors"
	"fmt"
)

// Service - сервис для работы с хранилищами.
// Осуществляет операции с хранилищами транзакционно, следит за консистентностью данных.
type Service struct {
	cfg *operation.Operation

	storagesMap map[string]storage.Driver // драйвера для работы с хранилищами
	driversMap  map[string]DriversMap     // поле для сопоставления драйвера хранения и конфигурации

	instanceID int

	// хранилище, куда сохранять транзакции (не кэш)
	storage txEditor
}

// txEditor - интерфейс для управления транзакциями: сохранение, удаление, и т.п.
type txEditor interface {
	storage.Driver
	txSaver
	txUpdater
	txDeleter
}

type txDeleter interface {
	DeleteTx(ctx context.Context, tx storage.TransactionEditor) error
}

// txSaver - интерфейс для сохранения транзакций в хранилище.
type txSaver interface {
	SaveTx(ctx context.Context, tx storage.TransactionEditor) error
}

type txUpdater interface {
	UpdateTx(ctx context.Context, tx storage.TransactionEditor) error
}

type DriversMap struct {
	driver storage.Driver
	cfg    operation.StorageCfg
}

type option func(*Service)

// WithStorages устанавливает драйвера для работы с хранилищами.
func WithStorages(storages []storage.Driver) option {
	return func(s *Service) {
		s.storagesMap = make(map[string]storage.Driver)

		for _, storage := range storages {
			s.storagesMap[string(storage.Name())] = storage
		}
	}
}

// WithCfg устанавливает конфигурацию операции.
func WithCfg(cfg *operation.Operation) option {
	return func(s *Service) {
		s.cfg = cfg
	}
}

// WithStorage устанавливает репозиторий для хранения транзакций.
func WithStorage(storage txEditor) option {
	return func(s *Service) {
		s.storage = storage
	}
}

func WithInstanceID(id int) option {
	return func(s *Service) {
		s.instanceID = id
	}
}

// New создает новый экземпляр сервиса.
// Возможные ошибки:
//   - cfg is required - не передан конфиг
//   - storages are required - не переданы драйвера для работы с хранилищами
//   - error mapping storages - ошибка при сопоставлении драйверов с конфигурациями хранилищ
func New(opts ...option) (*Service, error) {
	s := &Service{}

	for _, opt := range opts {
		opt(s)
	}

	if s.cfg == nil {
		return nil, errors.New("cfg is required")
	}

	if len(s.storagesMap) == 0 {
		return nil, errors.New("storages are required")
	}

	if s.storage == nil {
		return nil, errors.New("storage is required")
	}

	s.driversMap = make(map[string]DriversMap)

	if err := mapStoragesConfigs(s.driversMap, s.cfg.Storages, s.storagesMap); err != nil {
		return nil, fmt.Errorf("error mapping storages: %w", err)
	}

	return s, nil
}

func (s *Service) StoragesMap() map[string]DriversMap {
	return s.driversMap
}

// BuildRequests принимает на вход сообщение в виде мапы. Возвращает мапу с запросами для каждого драйвера.
func (s *Service) BuildRequests(msg map[string]interface{}, driversMap map[string]DriversMap, operation operation.Operation) (map[storage.Driver]*storage.Request, error) {
	res := make(map[storage.Driver]*storage.Request)

	for _, storage := range driversMap {
		var (
			builder builder_pkg.Builder
			err     error
		)

		builder, err = builderByStorageType(storage.driver.Type())
		if err != nil {
			return nil, fmt.Errorf("error get builder by storage type %q: %w", storage.driver.Type(), err)
		}

		builder = builder.WithOperation(operation).WithValues(msg).WithTable(storage.cfg.Table)

		builder, err = setOperationType(builder, operation.Type)
		if err != nil {
			return nil, fmt.Errorf("error set operation type %q: %w", operation.Type, err)
		}

		req, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("error build request for storage %q: %w", storage.cfg.Name, err)
		}

		res[storage.driver] = req
	}

	return res, nil
}

func builderByStorageType(storageType operation.StorageType) (builder_pkg.Builder, error) {
	switch storageType {
	case operation.StorageTypePostgres:
		return builder_pkg.ForPostgres(), nil
	default:
		return nil, fmt.Errorf("unknown storage type: %s", storageType)
	}
}

func setOperationType(builder builder_pkg.Builder, operationType operation.Type) (builder_pkg.Builder, error) {
	switch operationType {
	case operation.OperationTypeCreate:
		return builder.WithCreateOperation(), nil
	case operation.OperationTypeUpdate:
		return builder.WithUpdateOperation()
	default:
		return nil, fmt.Errorf("unknown operation type: %s", operationType)
	}
}

func mapStoragesConfigs(driversCfgMap map[string]DriversMap, storagesCfg []operation.StorageCfg, storagesMap map[string]storage.Driver) error {
	for _, cfg := range storagesCfg {
		driver, ok := storagesMap[string(cfg.Name)]
		if !ok {
			return fmt.Errorf("storage %q not found", cfg.Name)
		}

		driversCfgMap[cfg.Name] = DriversMap{
			driver: driver,
			cfg:    cfg,
		}
	}

	return nil
}
