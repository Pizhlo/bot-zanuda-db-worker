package storage

import "context"

// Driver определяет интерфейс для работы с хранилищем.
type Driver interface {
	Exec(ctx context.Context) error
	Close() error
}
