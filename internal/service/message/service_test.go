package message

import (
	interfaces "db-worker/internal/service/message/interface"
	"db-worker/internal/service/message/mocks"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	createNotesHandler := mocks.NewMockHandler(ctrl)
	createNotesChan := make(chan interfaces.Message)
	updateNotesChan := make(chan interfaces.Message)
	updateNotesHandler := mocks.NewMockHandler(ctrl)

	tests := []struct {
		name            string
		opts            []ServiceOption
		expectedService *Service
		err             error
	}{
		{
			name: "positive case",
			opts: []ServiceOption{
				WithCreateChannels([]chan interfaces.Message{createNotesChan}),
				WithCreateHandler(createNotesHandler),
				WithUpdateChannels([]chan interfaces.Message{updateNotesChan}),
				WithUpdateHandler(updateNotesHandler),
			},
			expectedService: &Service{
				createChannels: []chan interfaces.Message{createNotesChan},
				createHandler:  createNotesHandler,
				updateChannels: []chan interfaces.Message{updateNotesChan},
				updateHandler:  updateNotesHandler,
			},
		},
		{
			name: "negative case: create channels is nil",
			opts: []ServiceOption{
				WithCreateHandler(createNotesHandler),
				WithUpdateChannels([]chan interfaces.Message{updateNotesChan}),
				WithUpdateHandler(updateNotesHandler),
			},
			err: errors.New("create channels is required"),
		},
		{
			name: "negative case: createHandler is nil",
			opts: []ServiceOption{
				WithCreateChannels([]chan interfaces.Message{createNotesChan}),
				WithUpdateChannels([]chan interfaces.Message{updateNotesChan}),
				WithUpdateHandler(updateNotesHandler),
			},
			err: errors.New("create handler is required"),
		},
		{
			name: "negative case: update channels is nil",
			opts: []ServiceOption{
				WithCreateChannels([]chan interfaces.Message{createNotesChan}),
				WithCreateHandler(createNotesHandler),
				WithUpdateHandler(updateNotesHandler),
			},
			err: errors.New("update channels is required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, err := New(tt.opts...)
			if tt.err != nil {
				require.Error(t, err)
				require.EqualError(t, tt.err, err.Error())
				assert.Nil(t, service)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedService, service)
			}
		})
	}
}
