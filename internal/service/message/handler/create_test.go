package handler

import (
	"errors"
	"testing"

	mocks "db-worker/internal/service/message/handler/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCreateNoteHandler(t *testing.T) {
	type testCase struct {
		name string
		opts []createHandlerOption
		want error
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockNotesStorage := mocks.NewMocknoteCreator(mockCtrl)

	tests := []testCase{
		{
			name: "success",
			opts: []createHandlerOption{
				WithBufferSizeCreateHandler(10),
				WithNotesCreator(mockNotesStorage),
			},
			want: nil,
		},

		{
			name: "without buffer size",
			opts: []createHandlerOption{
				WithNotesCreator(mockNotesStorage),
			},
			want: errors.New("buffer size is 0"),
		},
		{
			name: "without notes storage",
			opts: []createHandlerOption{
				WithBufferSizeCreateHandler(10),
			},
			want: errors.New("notes storage is nil"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewCreateHandler(tt.opts...)
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

// func TestHandleCreateNote(t *testing.T) {
// 	t.Parallel()

// 	type testCase struct {
// 		name          string
// 		msg           interfaces.Message
// 		expectedNotes []interfaces.Message
// 		expectedCount int
// 		shouldSave    bool
// 		buffer        *buffer.Buffer
// 		want          error
// 	}

// 	note := model.CreateNoteRequest{
// 		Text: "test",
// 		ID:   uuid.New(),
// 	}

// 	tests := []testCase{
// 		{
// 			name:          "success with empty buffer",
// 			msg:           note,
// 			shouldSave:    true,
// 			buffer:        buffer.New(10),
// 			expectedCount: 1,
// 		},
// 		{
// 			name:          "success with full buffer && should save",
// 			msg:           note,
// 			shouldSave:    true,
// 			buffer:        createAndFillBuffer(t, 2, []interfaces.Message{note}),
// 			expectedNotes: []interfaces.Message{note, note},
// 			expectedCount: 2,
// 		},
// 		{
// 			name:          "success with full buffer && should not save",
// 			msg:           note,
// 			shouldSave:    false,
// 			buffer:        buffer.New(1),
// 			expectedNotes: []interfaces.Message{note},
// 			expectedCount: 1,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			done := make(chan struct{})

// 			storage := &mockStorage{
// 				t:             t,
// 				expectedCount: tt.expectedCount,
// 				createdNotes:  make([]interfaces.Message, 0, len(tt.expectedNotes)),
// 				done:          done,
// 			}

// 			handler, err := NewCreateHandler(WithBufferSizeCreateHandler(10), WithNotesCreator(storage))
// 			require.NoError(t, err)

// 			handler.buffer = tt.buffer

// 			err = handler.Handle(t.Context(), tt.msg, tt.shouldSave)
// 			if tt.want != nil {
// 				require.EqualError(t, err, tt.want.Error())
// 			} else {
// 				require.NoError(t, err)
// 				// ждем завершения горутины
// 				storage.WaitForSave()

// 				if len(tt.expectedNotes) > 0 {
// 					for {
// 						select {
// 						case <-done:
// 							// проверяем, что сохранилось нужное количество заметок
// 							assert.Equal(t, len(tt.expectedNotes), len(storage.GetCreatedNotes()))
// 							// проверяем, что сохранились правильные заметки
// 							assert.Equal(t, tt.expectedNotes, storage.GetCreatedNotes())
// 							return
// 						case <-time.After(1 * time.Second):
// 							t.Fatalf("timeout")
// 						}
// 					}
// 				}
// 			}
// 		})
// 	}
// }

// type mockStorage struct {
// 	t             *testing.T
// 	mu            sync.Mutex
// 	wg            sync.WaitGroup
// 	expectedCount int
// 	done          chan struct{}
// 	createdNotes  []interfaces.Message
// 	updatedNotes  []interfaces.Message
// }

// func (m *mockStorage) SaveNotes(ctx context.Context, notes []interfaces.Message) {
// 	m.t.Helper()

// 	m.wg.Add(1)
// 	defer m.wg.Done()

// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	m.createdNotes = append(m.createdNotes, notes...)

// 	if len(m.createdNotes) == m.expectedCount {
// 		m.done <- struct{}{}
// 	}
// }

// func (m *mockStorage) UpdateNotes(ctx context.Context, notes []interfaces.Message) {
// 	m.t.Helper()

// 	m.wg.Add(1)
// 	defer m.wg.Done()

// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	m.updatedNotes = append(m.updatedNotes, notes...)

// 	if len(m.updatedNotes) == m.expectedCount {
// 		m.done <- struct{}{}
// 	}
// }

// func (m *mockStorage) WaitForSave() {
// 	m.wg.Wait()
// }

// func (m *mockStorage) GetCreatedNotes() []interfaces.Message {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	return m.createdNotes
// }

// func (m *mockStorage) GetUpdatedNotes() []interfaces.Message {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	return m.updatedNotes
// }

// func createAndFillBuffer(t *testing.T, bufferSize int, notes []interfaces.Message) *buffer.Buffer {
// 	t.Helper()

// 	buf := buffer.New(bufferSize)

// 	for _, note := range notes {
// 		err := buf.Add(note)
// 		require.NoError(t, err)
// 	}

// 	return buf
// }
