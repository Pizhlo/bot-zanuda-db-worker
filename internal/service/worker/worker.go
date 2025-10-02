package worker

import (
	"context"

	interfaces "db-worker/internal/service/message/interface"
)

type Worker interface {
	Name() string
	Run(ctx context.Context) error
	Close() error
	MsgChan() chan interfaces.Message
}
