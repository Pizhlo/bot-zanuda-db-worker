package storage

import (
	"context"
	"db-worker/internal/config/operation"
)

// Driver определяет интерфейс для работы с хранилищем.
type Driver interface {
	Run(ctx context.Context) error
	Exec(ctx context.Context, req *Request) error
	Stop(ctx context.Context) error
	Type() operation.StorageType
	Name() string
}

// Request - запрос к хранилищу.
// Val - запрос, который может быть разным в зависимости от хранилища.
// Args - аргументы для запроса (по необходимости).
type Request struct {
	Val  any
	Args any
}
