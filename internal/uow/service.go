package uow

import (
	"db-worker/internal/config/operation"
	builder_pkg "db-worker/internal/service/builder"
	"db-worker/internal/storage"
	"errors"
	"fmt"
	"sync"
)

// Service - сервис для работы с хранилищами.
// Осуществляет операции с хранилищами транзакционно, следит за консистентностью данных.
type Service struct {
	cfg *operation.Operation

	mu           sync.RWMutex
	transactions map[string]*transaction // транзакции, которые начаты

	storagesMap map[string]storage.Driver // драйвера для работы с хранилищами
	driversMap  map[string]drivers        // поле для сопоставления драйвера хранения и конфигурации
}

type drivers struct {
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

	if err := s.mapStorages(); err != nil {
		return nil, fmt.Errorf("error mapping storages: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.transactions = make(map[string]*transaction)

	return s, nil
}

func (s *Service) mapStorages() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.driversMap = make(map[string]drivers)

	for _, storage := range s.cfg.Storages {
		driver, ok := s.storagesMap[string(storage.Name)]
		if !ok {
			return fmt.Errorf("storage %q not found", storage.Name)
		}

		s.driversMap[storage.Name] = drivers{
			driver: driver,
			cfg:    storage,
		}
	}

	return nil
}

// BuildRequests принимает на вход сообщение в виде мапы. Возвращает мапу с запросами для каждого драйвера.
func (s *Service) BuildRequests(msg map[string]interface{}) (map[storage.Driver]*storage.Request, error) {
	res := make(map[storage.Driver]*storage.Request)

	for _, storage := range s.driversMap {
		var (
			builder builder_pkg.Builder
			err     error
		)

		builder, err = builderByStorageType(storage.driver.Type())
		if err != nil {
			return nil, fmt.Errorf("error get builder by storage type %q: %w", storage.driver.Type(), err)
		}

		builder = builder.WithOperation(*s.cfg).WithValues(msg).WithTable(storage.cfg.Table)

		builder, err = setOperationType(builder, s.cfg.Type)
		if err != nil {
			return nil, fmt.Errorf("error set operation type %q: %w", s.cfg.Type, err)
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
