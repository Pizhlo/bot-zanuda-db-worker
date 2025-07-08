package handler

import (
	"context"
	buffer "db-worker/internal/service/message/buffer"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"

	"github.com/sirupsen/logrus"
)

type CreateNoteHandler struct {
	buffer     *buffer.Buffer
	bufferSize int

	notesStorage noteCreator // репозиторий для работы с заметками
}

// интерфейс для работы с заметками.
//
//go:generate mockgen -source=create_note.go -destination=./mocks/create_note_mock.go -package=handler NoteCreator
type noteCreator interface {
	SaveNotes(ctx context.Context, note []interfaces.Message)
}

type createNoteHandlerOption func(*CreateNoteHandler)

func WithBufferSizeCreateNoteHandler(bufferSize int) createNoteHandlerOption {
	return func(h *CreateNoteHandler) {
		h.bufferSize = bufferSize
	}
}

func WithNotesCreator(notesStorage noteCreator) createNoteHandlerOption {
	return func(h *CreateNoteHandler) {
		h.notesStorage = notesStorage
	}
}

func NewCreateNoteHandler(opts ...createNoteHandlerOption) (*CreateNoteHandler, error) {
	h := &CreateNoteHandler{}

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

func (h *CreateNoteHandler) Handle(ctx context.Context, msg interfaces.Message, shouldSave bool) error {
	logrus.Debugf("create note handler: handle message: %+v", msg)

	err := h.buffer.Add(msg)
	if err != nil {
		return fmt.Errorf("create note handler: error add message to buffer: %+v", err)
	}

	// если буфер заполнен или нужно сохранить сообщение, то отправляем его в БД
	if h.buffer.IsFull() || shouldSave {
		notes := h.buffer.Get()

		logrus.Debugf("create note handler: buffer is full, saving %d notes", len(notes))

		go h.notesStorage.SaveNotes(ctx, notes)

		h.buffer.Flush()
	}

	return nil
}
