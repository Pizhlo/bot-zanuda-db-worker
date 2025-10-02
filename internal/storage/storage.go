package storage

import "context"

type StorageDriver interface {
	Exec(ctx context.Context) error
	Close() error
}
