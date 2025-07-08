package handler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"db-worker/internal/model"
	buffer "db-worker/internal/service/message/buffer"
	mocks "db-worker/internal/service/message/handler/mocks"
	interfaces "db-worker/internal/service/message/interface"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCreateNoteHandler(t *testing.T) {
	type testCase struct {
		name string
		opts []createNoteHandlerOption
		want error
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockNotesStorage := mocks.NewMocknoteCreator(mockCtrl)

	tests := []testCase{
		{
			name: "success",
			opts: []createNoteHandlerOption{
				WithBufferSize(10),
				WithNotesStorage(mockNotesStorage),
			},
			want: nil,
		},

		{
			name: "without buffer size",
			opts: []createNoteHandlerOption{
				WithNotesStorage(mockNotesStorage),
			},
			want: errors.New("buffer size is 0"),
		},
		{
			name: "without notes storage",
			opts: []createNoteHandlerOption{
				WithBufferSize(10),
			},
			want: errors.New("notes storage is nil"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewCreateNoteHandler(tt.opts...)
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

func TestHandle(t *testing.T) {
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

	note := model.CreateNoteRequest{
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
				savedNotes:    make([]interfaces.Message, 0, len(tt.expectedNotes)),
				done:          done,
			}

			handler, err := NewCreateNoteHandler(WithBufferSize(10), WithNotesStorage(storage))
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
							assert.Equal(t, len(tt.expectedNotes), len(storage.GetSavedNotes()))
							// проверяем, что сохранились правильные заметки
							assert.Equal(t, tt.expectedNotes, storage.GetSavedNotes())
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

type mockStorage struct {
	t             *testing.T
	mu            sync.Mutex
	wg            sync.WaitGroup
	expectedCount int
	done          chan struct{}
	savedNotes    []interfaces.Message
}

func (m *mockStorage) SaveNotes(ctx context.Context, notes []interfaces.Message) {
	m.t.Helper()

	m.wg.Add(1)
	defer m.wg.Done()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.savedNotes = append(m.savedNotes, notes...)

	if len(m.savedNotes) == m.expectedCount {
		m.done <- struct{}{}
	}
}

func (m *mockStorage) WaitForSave() {
	m.wg.Wait()
}

func (m *mockStorage) GetSavedNotes() []interfaces.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.savedNotes
}

func createAndFillBuffer(t *testing.T, bufferSize int, notes []interfaces.Message) *buffer.Buffer {
	t.Helper()

	buf := buffer.New(bufferSize)

	for _, note := range notes {
		err := buf.Add(note)
		require.NoError(t, err)
	}

	return buf
}
