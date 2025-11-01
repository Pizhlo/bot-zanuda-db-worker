package model

import (
	"context"
	"db-worker/internal/config/operation"
)

// Driver определяет интерфейс для работы с хранилищем.
//
//go:generate mockgen -source=driver.go -destination=../mocks/driver_mock.go -package=mocks Driver
type Driver interface {
	configurator
	transactionEditor
	runner
}

type transactionEditor interface {
	Commit(ctx context.Context, id string) error
	Rollback(ctx context.Context, id string) error
	Begin(ctx context.Context, id string) error
	// FinishTx завершает транзакцию. Используется, если не удалось начать транзакцию в одном из драйверов.
	FinishTx(ctx context.Context, id string) error
}

type runner interface {
	Run(ctx context.Context) error
	Exec(ctx context.Context, req *Request, id string) error
	Stop(ctx context.Context) error
}

type configurator interface {
	Type() operation.StorageType
	Name() string
	Table() string
	Host() string
	Port() int
	User() string
	Password() string
	DBName() string
	Queue() string
	RoutingKey() string
	InsertTimeout() int
	ReadTimeout() int
}
