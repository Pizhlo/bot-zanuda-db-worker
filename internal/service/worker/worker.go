package worker

import (
	"context"
)

// Worker определяет интерфейс для работы с workers.
type Worker interface {
	Name() string
	Connect() error
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
	MsgChan() chan map[string]interface{}
	Address() string
	Queue() string
	RoutingKey() string
	InsertTimeout() int
	ReadTimeout() int
}
