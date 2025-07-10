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
				WithCreateNotesChan(createNotesChan),
				WithCreateHandler(createNotesHandler),
				WithUpdateNotesChan(updateNotesChan),
				WithUpdateHandler(updateNotesHandler),
			},
			expectedService: &Service{
				createNotesChan: createNotesChan,
				createHandler:   createNotesHandler,
				updateNotesChan: updateNotesChan,
				updateHandler:   updateNotesHandler,
			},
		},
		{
			name: "negative case: createNotesChan is nil",
			opts: []ServiceOption{
				WithCreateHandler(createNotesHandler),
				WithUpdateNotesChan(updateNotesChan),
				WithUpdateHandler(updateNotesHandler),
			},
			err: errors.New("create notes channel is required"),
		},
		{
			name: "negative case: createHandler is nil",
			opts: []ServiceOption{
				WithCreateNotesChan(createNotesChan),
				WithUpdateNotesChan(updateNotesChan),
				WithUpdateHandler(updateNotesHandler),
			},
			err: errors.New("create handler is required"),
		},
		{
			name: "negative case: updateNotesChan is nil",
			opts: []ServiceOption{
				WithCreateNotesChan(createNotesChan),
				WithCreateHandler(createNotesHandler),
				WithUpdateHandler(updateNotesHandler),
			},
			err: errors.New("update notes channel is required"),
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
