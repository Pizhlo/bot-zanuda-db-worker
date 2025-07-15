package handler

import (
	"db-worker/internal/model"
	buffer "db-worker/internal/service/message/buffer"
	mocks "db-worker/internal/service/message/handler/mocks"
	interfaces "db-worker/internal/service/message/interface"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUpdateNoteHandler(t *testing.T) {
	type testCase struct {
		name string
		opts []updateHandlerOption
		want error
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockNotesStorage := mocks.NewMocknoteUpdater(mockCtrl)

	tests := []testCase{
		{
			name: "success",
			opts: []updateHandlerOption{
				WithBufferSizeUpdateHandler(10),
				WithNotesUpdater(mockNotesStorage),
			},
			want: nil,
		},

		{
			name: "without buffer size",
			opts: []updateHandlerOption{
				WithNotesUpdater(mockNotesStorage),
			},
			want: errors.New("buffer size is 0"),
		},
		{
			name: "without notes storage",
			opts: []updateHandlerOption{
				WithBufferSizeUpdateHandler(10),
			},
			want: errors.New("notes storage is nil"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewUpdateHandler(tt.opts...)
			if tt.want != nil {
				require.EqualError(t, err, tt.want.Error())
				assert.Nil(t, handler)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, handler)
			}
		})
	}
}

func TestHandleUpdateNote(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		msg           interfaces.Message
		expectedNotes []interfaces.Message
		expectedCount int
		shouldSave    bool
		buffer        *buffer.Buffer
		want          error
	}

	note := model.UpdateNoteRequest{
		Text: "test",
		ID:   uuid.New(),
	}

	tests := []testCase{
		{
			name:          "success with empty buffer",
			msg:           note,
			shouldSave:    true,
			buffer:        buffer.New(10),
			expectedCount: 1,
		},
		{
			name:          "success with full buffer && should save",
			msg:           note,
			shouldSave:    true,
			buffer:        createAndFillBuffer(t, 2, []interfaces.Message{note}),
			expectedNotes: []interfaces.Message{note, note},
			expectedCount: 2,
		},
		{
			name:          "success with full buffer && should not save",
			msg:           note,
			shouldSave:    false,
			buffer:        buffer.New(1),
			expectedNotes: []interfaces.Message{note},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})

			storage := &mockStorage{
				t:             t,
				expectedCount: tt.expectedCount,
				updatedNotes:  make([]interfaces.Message, 0, len(tt.expectedNotes)),
				done:          done,
			}

			handler, err := NewUpdateHandler(WithBufferSizeUpdateHandler(10), WithNotesUpdater(storage))
			require.NoError(t, err)

			handler.buffer = tt.buffer

			err = handler.Handle(t.Context(), tt.msg, tt.shouldSave)
			if tt.want != nil {
				require.EqualError(t, err, tt.want.Error())
			} else {
				require.NoError(t, err)
				// ждем завершения горутины
				storage.WaitForSave()

				if len(tt.expectedNotes) > 0 {
					for {
						select {
						case <-done:
							// проверяем, что сохранилось нужное количество заметок
							assert.Equal(t, len(tt.expectedNotes), len(storage.GetUpdatedNotes()))
							// проверяем, что сохранились правильные заметки
							assert.Equal(t, tt.expectedNotes, storage.GetUpdatedNotes())
							return
						case <-time.After(1 * time.Second):
							t.Fatalf("timeout")
						}
					}
				}
			}
		})
	}
}
