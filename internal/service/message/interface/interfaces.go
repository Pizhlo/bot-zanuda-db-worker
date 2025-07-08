package interfaces

import (
	"context"
	"db-worker/internal/model"

	"github.com/google/uuid"
)

// Handler определяет интерфейс для обработчиков сообщений
//
//go:generate mockgen -source=interfaces.go -destination=../mocks/mock_handler.go -package=mocks
type Handler interface {
	Handle(ctx context.Context, msg Message, shouldSave bool) error
}

// Message определяет интерфейс для сообщений.
type Message interface {
	MessageType() model.MessageType
	Model() any
	GetRequestID() uuid.UUID
	GetOperation() model.Operation
}
