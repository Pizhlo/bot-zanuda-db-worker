package message

import (
	"context"
	"db-worker/internal/service/operation/message"
)

// Get получает сообщение по id.
func (r *Repo) Get(ctx context.Context, id string) (message.Message, error) {
	return message.Message{}, nil
}

// GetAll получает все сообщения.
func (r *Repo) GetAll(ctx context.Context) ([]message.Message, error) {
	return []message.Message{}, nil
}
