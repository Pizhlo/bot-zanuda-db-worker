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
	cfg          *operation.Operation
	requestsRepo requestsRepo

	userStoragesMap map[string]storage.Driver // драйвера для работы с хранилищами
	userDriversMap  map[string]DriversMap     // поле для сопоставления драйвера хранения и конфигурации

	instanceID int

	// хранилище, куда сохранять транзакции (не кэш)
	storage storage.Driver

	transactionDriversMap map[string]DriversMap     // системные хранилища, куда сохраняется состояние транзакции
	systemStoragesMap     map[string]storage.Driver // системные хранилища, куда сохраняется состояние транзакции

	requestsDriversMap map[string]DriversMap // системные хранилища, куда сохраняются запросы

	systemStorageConfigs []operation.StorageCfg // конфигурация системных хранилищ (кэш, БД)
}

//go:generate mockgen -source=service.go -destination=mocks/mocks.go -package=mocks
type requestsRepo interface {
	// GetAllTransactionsByFields получает все транзакции, удовлетворяющие условию.
	// fields - мапа с полями и значениями для фильтрации.
	GetAllTransactionsByFields(ctx context.Context, fields map[string]any) ([]storage.TransactionModel, error)
	// UpdateStatusMany обновляет статус транзакций по айдишникам.
	UpdateStatusMany(ctx context.Context, ids []string, status string, errMsg string) error
}

// DriversMap - структура, связывающая драйвер хранилища и его конфигурацию.
type DriversMap struct {
	driver storage.Driver
	cfg    operation.StorageCfg
}

type option func(*Service)

// WithStorages устанавливает драйвера для работы с хранилищами.
func WithStorages(storages []storage.Driver) option {
	return func(s *Service) {
		for _, storage := range storages {
			s.userStoragesMap[string(storage.Name())] = storage
		}
	}
}

// WithCfg устанавливает конфигурацию операции.
func WithCfg(cfg *operation.Operation) option {
	return func(s *Service) {
		s.cfg = cfg
	}
}

const (
	// StorageNameForTransactionsTable - константа для сохранения конфига системного хранилища для таблицы транзакций.
	StorageNameForTransactionsTable = "system-storage.transactions-table"

	// StorageNameForRequestsTable - константа для сохранения конфига системного хранилища для таблицы запросов.
	StorageNameForRequestsTable = "system-storage.requests-table"
)

// WithStorage устанавливает репозиторий для хранения транзакций.
func WithStorage(systemStorage storage.Driver) option {
	return func(s *Service) {
		s.storage = systemStorage

		// сохраняем одно соединение под двумя разными названиями: для таблицы transactions.transactions и transactions.requests
		s.systemStoragesMap[StorageNameForTransactionsTable] = systemStorage
		s.systemStoragesMap[StorageNameForRequestsTable] = systemStorage
	}
}

// WithInstanceID устанавливает идентификатор экземпляра приложения.
func WithInstanceID(id int) option {
	return func(s *Service) {
		s.instanceID = id
	}
}

// WithSystemStorageConfigs устанавливает конфигурации системных хранилищ.
func WithSystemStorageConfigs(configs []operation.StorageCfg) option {
	return func(s *Service) {
		s.systemStorageConfigs = configs
	}
}

// WithRequestsRepo устанавливает репозиторий для работы с запросами.
func WithRequestsRepo(repo requestsRepo) option {
	return func(s *Service) {
		s.requestsRepo = repo
	}
}

// New создает новый экземпляр сервиса.
// Возможные ошибки:
//   - cfg is required - не передан конфиг
//   - storages are required - не переданы драйвера для работы с хранилищами
//   - error mapping storages - ошибка при сопоставлении драйверов с конфигурациями хранилищ
func New(opts ...option) (*Service, error) {
	s := &Service{
		systemStoragesMap:     make(map[string]storage.Driver),
		transactionDriversMap: make(map[string]DriversMap),
		userDriversMap:        make(map[string]DriversMap),
		userStoragesMap:       make(map[string]storage.Driver),
		instanceID:            0,
		cfg:                   nil,
		storage:               nil,
		systemStorageConfigs:  make([]operation.StorageCfg, 0),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.cfg == nil {
		return nil, errors.New("cfg is required")
	}

	if len(s.userStoragesMap) == 0 {
		return nil, errors.New("storages are required")
	}

	if s.storage == nil {
		return nil, errors.New("storage is required")
	}

	if len(s.systemStorageConfigs) == 0 {
		return nil, errors.New("system storage configs are required")
	}

	if s.requestsRepo == nil {
		return nil, errors.New("requests repo is required")
	}

	s.userDriversMap = make(map[string]DriversMap)

	if err := mapStoragesConfigs(s.userDriversMap, s.cfg.Storages, s.userStoragesMap); err != nil {
		return nil, fmt.Errorf("error mapping user storages: %w", err)
	}

	s.transactionDriversMap = make(map[string]DriversMap)

	if err := mapStoragesConfigs(s.transactionDriversMap, s.systemStorageConfigs, s.systemStoragesMap); err != nil {
		return nil, fmt.Errorf("error mapping system storages: %w", err)
	}

	s.requestsDriversMap = make(map[string]DriversMap)

	s.requestsDriversMap[StorageNameForRequestsTable] = s.transactionDriversMap[StorageNameForRequestsTable]
	delete(s.transactionDriversMap, StorageNameForRequestsTable)

	return s, nil
}

// StoragesMap возвращает мапу с драйверами для работы с хранилищами.
func (s *Service) StoragesMap() map[string]DriversMap {
	return s.userDriversMap
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
	case operation.OperationTypeDelete:
		return builder.WithDeleteOperation()
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
