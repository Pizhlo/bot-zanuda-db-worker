package buffer

import (
	"db-worker/internal/model"
	interfaces "db-worker/internal/service/message/interface"
	"fmt"
	"testing"

	"bou.ke/monkey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	buffer := New(100)
	assert.NotNil(t, buffer)
	assert.Equal(t, 100, buffer.bufferSize)
	assert.Empty(t, buffer.buff)
	assert.False(t, buffer.IsFull())
	assert.Equal(t, []interfaces.Message{}, buffer.Get())
}

func TestAdd(t *testing.T) {
	generatedID := uuid.New()

	uuidPatch := monkey.Patch(uuid.New, func() uuid.UUID { return generatedID })
	defer uuidPatch.Unpatch()

	tests := []struct {
		name         string
		bufferSize   int
		msg          interfaces.Message
		startBuff    []interfaces.Message
		expectedBuff []interfaces.Message
		wantErr      bool
		err          error
	}{
		{
			name:       "positive case: empty buffer",
			bufferSize: 100,
			msg: model.CreateNoteRequest{
				ID:        uuid.New(),
				RequestID: uuid.New(),
				UserID:    1,
				SpaceID:   uuid.New(),
				Text:      "test",
				Type:      model.TextNoteType,
				Operation: model.CreateOp,
			},
			startBuff: []interfaces.Message{},
			expectedBuff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
		},
		{
			name:       "positive case: buffer not empty",
			bufferSize: 100,
			msg: model.CreateNoteRequest{
				ID:        uuid.New(),
				RequestID: uuid.New(),
				UserID:    1,
				SpaceID:   uuid.New(),
				Text:      "test4",
				Type:      model.TextNoteType,
				Operation: model.CreateOp,
			},
			startBuff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test1",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test2",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test3",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
			expectedBuff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test1",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test2",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test3",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test4",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
		},
		{
			name:       "negative case: buffer is full",
			bufferSize: 3,
			msg: model.CreateNoteRequest{
				ID:        uuid.New(),
				RequestID: uuid.New(),
				UserID:    1,
				SpaceID:   uuid.New(),
				Text:      "test4",
				Type:      model.TextNoteType,
				Operation: model.CreateOp,
			},
			startBuff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test1",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test2",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test3",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
			expectedBuff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test1",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test2",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test3",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
			wantErr: true,
			err:     fmt.Errorf("buffer is full"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := New(tt.bufferSize)
			buffer.buff = tt.startBuff

			err := buffer.Add(tt.msg)
			if tt.wantErr {
				require.Error(t, err)
				assert.EqualError(t, tt.err, err.Error())
			} else {
				require.Equal(t, tt.expectedBuff, buffer.Get())
				require.NoError(t, err)
			}
		})
	}
}

func TestFlush(t *testing.T) {
	buffer := New(100)

	_ = buffer.Add(model.CreateNoteRequest{
		ID:        uuid.New(),
		RequestID: uuid.New(),
		UserID:    1,
		SpaceID:   uuid.New(),
		Text:      "test",
		Type:      model.TextNoteType,
		Operation: model.CreateOp,
	})

	buffer.Flush()
	assert.Empty(t, buffer.Get())
}

func TestGet(t *testing.T) {
	note := model.CreateNoteRequest{
		ID:        uuid.New(),
		RequestID: uuid.New(),
		UserID:    1,
		SpaceID:   uuid.New(),
		Text:      "test",
		Type:      model.TextNoteType,
		Operation: model.CreateOp,
	}

	buffer := New(100)
	_ = buffer.Add(note)

	buff := buffer.Get()
	require.Equal(t, []interfaces.Message{note}, buff)
}

func TestIsFull(t *testing.T) {
	type testCase struct {
		name       string
		bufferSize int
		buff       []interfaces.Message
		expected   bool
	}

	tests := []testCase{
		{
			name:       "positive case: buffer is not full",
			bufferSize: 4,
			buff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test1",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test2",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test3",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
			expected: false,
		},
		{
			name:       "positive case: buffer is full",
			bufferSize: 3,
			buff: []interfaces.Message{
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test1",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test2",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
				model.CreateNoteRequest{
					ID:        uuid.New(),
					RequestID: uuid.New(),
					UserID:    1,
					SpaceID:   uuid.New(),
					Text:      "test3",
					Type:      model.TextNoteType,
					Operation: model.CreateOp,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := New(tt.bufferSize)
			buffer.buff = tt.buff
			require.Equal(t, tt.expected, buffer.IsFull())
		})
	}
}
