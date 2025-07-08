package handler

import (
	"context"
	buffer "db-worker/internal/service/message/buffer"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
)

type UpdateNoteHandler struct {
	buffer     *buffer.Buffer
	bufferSize int

	notesStorage noteUpdater // репозиторий для работы с заметками
}

// интерфейс для работы с заметками.
//
//go:generate mockgen -source=update_handler.go -destination=./mocks/update_handler_mock.go -package=handler NoteUpdater
type noteUpdater interface {
	UpdateNotes(ctx context.Context, note []interfaces.Message)
}

type updateNoteHandlerOption func(*UpdateNoteHandler)

func WithBufferSizeUpdateNoteHandler(bufferSize int) updateNoteHandlerOption {
	return func(h *UpdateNoteHandler) {
		h.bufferSize = bufferSize
	}
}

func WithNotesUpdater(notesStorage noteUpdater) updateNoteHandlerOption {
	return func(h *UpdateNoteHandler) {
		h.notesStorage = notesStorage
	}
}

func NewUpdateNoteHandler(opts ...updateNoteHandlerOption) (*UpdateNoteHandler, error) {
	h := &UpdateNoteHandler{}

	for _, opt := range opts {
		opt(h)
	}

	if h.bufferSize == 0 {
		return nil, fmt.Errorf("buffer size is 0")
	}

	if h.notesStorage == nil {
		return nil, fmt.Errorf("notes storage is nil")
	}

	h.buffer = buffer.New(h.bufferSize)

	return h, nil
}

func (h *UpdateNoteHandler) Handle(ctx context.Context, msg interfaces.Message, shouldSave bool) error {
	logrus.Debugf("update note handler: handle message: %+v", msg)

	err := h.buffer.Add(msg)
	if err != nil {
		return fmt.Errorf("update note handler: error add message to buffer: %+v", err)
	}

	// если буфер заполнен или нужно сохранить сообщение, то отправляем его в БД
	if h.buffer.IsFull() || shouldSave {
		notes := h.buffer.Get()

		logrus.Debugf("update note handler: buffer is full, saving %d notes", len(notes))

		go h.notesStorage.UpdateNotes(ctx, notes)

		h.buffer.Flush()
	}

	return nil
}
