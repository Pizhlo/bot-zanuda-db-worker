package interfaces

import (
	"context"
)

// Handler определяет интерфейс для обработчиков сообщений
//
//go:generate mockgen -source=interfaces.go -destination=../mocks/mock_handler.go -package=mocks
type Handler interface {
	Handle(ctx context.Context, msg Message, shouldSave bool) error
}

// Message определяет интерфейс для сообщений.
type Message interface {
}
