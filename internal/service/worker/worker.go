package worker

import (
	"context"

	interfaces "db-worker/internal/service/message/interface"
)

// Worker определяет интерфейс для работы с workers.
type Worker interface {
	Name() string
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
	MsgChan() chan interfaces.Message
}
