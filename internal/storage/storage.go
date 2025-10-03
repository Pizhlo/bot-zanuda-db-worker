package storage

import "context"

// Driver определяет интерфейс для работы с хранилищем.
type Driver interface {
	Run(ctx context.Context) error
	Exec(ctx context.Context) error
	Stop(ctx context.Context) error
}
