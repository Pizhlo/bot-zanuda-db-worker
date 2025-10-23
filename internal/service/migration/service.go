package migration

import (
	"context"
	"errors"
)

// Service - сервис для загрузки миграций.
type Service struct {
	loader migrationLoader
}

// migrationLoader - интерфейс для загрузки миграций.
type migrationLoader interface {
	Load(ctx context.Context) error
}

type option func(*Service)

// WithMigrationLoader устанавливает репозиторий для загрузки миграций.
func WithMigrationLoader(loader migrationLoader) option {
	return func(s *Service) {
		s.loader = loader
	}
}

// New создает новый экземпляр сервиса для загрузки миграций.
func New(opts ...option) (*Service, error) {
	s := &Service{}
	for _, opt := range opts {
		opt(s)
	}

	if s.loader == nil {
		return nil, errors.New("loader is required")
	}

	return s, nil
}

// Run загружает миграции.
func (s *Service) Run(ctx context.Context) error {
	return s.loader.Load(ctx)
}

// Stop закрывает сервис.
func (s *Service) Stop(_ context.Context) error {
	return nil
}
