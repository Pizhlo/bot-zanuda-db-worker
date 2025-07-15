package handler

import (
	"context"
	buffer "db-worker/internal/service/message/buffer"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
)

type CreateHandler struct {
	buffer     *buffer.Buffer
	bufferSize int

	storage storageCreator // репозиторий для работы с заметками
}

// интерфейс для работы с заметками.
//
//go:generate mockgen -source=create_note.go -destination=./mocks/create_note_mock.go -package=handler NoteCreator
type storageCreator interface {
	SaveNotes(ctx context.Context, note []interfaces.Message)
}

type createHandlerOption func(*CreateHandler)

func WithBufferSizeCreateHandler(bufferSize int) createHandlerOption {
	return func(h *CreateHandler) {
		h.bufferSize = bufferSize
	}
}

func WithNotesCreator(notesStorage storageCreator) createHandlerOption {
	return func(h *CreateHandler) {
		h.storage = notesStorage
	}
}

func NewCreateHandler(opts ...createHandlerOption) (*CreateHandler, error) {
	h := &CreateHandler{}

	for _, opt := range opts {
		opt(h)
	}

	if h.bufferSize == 0 {
		return nil, fmt.Errorf("buffer size is 0")
	}

	if h.storage == nil {
		return nil, fmt.Errorf("notes storage is nil")
	}

	h.buffer = buffer.New(h.bufferSize)

	return h, nil
}

func (h *CreateHandler) Handle(ctx context.Context, msg interfaces.Message, shouldSave bool) error {
	logrus.Debugf("create handler: handle message: %+v", msg)

	err := h.buffer.Add(msg)
	if err != nil {
		return fmt.Errorf("create handler: error add message to buffer: %+v", err)
	}

	// если буфер заполнен или нужно сохранить сообщение, то отправляем его в БД
	if h.buffer.IsFull() || shouldSave {
		notes := h.buffer.Get()

		logrus.Debugf("create handler: buffer is full, saving %d records", len(notes))

		go h.storage.SaveNotes(ctx, notes)

		h.buffer.Flush()
	}

	return nil
}
