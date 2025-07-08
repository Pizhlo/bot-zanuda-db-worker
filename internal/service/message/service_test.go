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

	createHandler := mocks.NewMockHandler(ctrl)
	msgChan := make(chan interfaces.Message)

	tests := []struct {
		name            string
		opts            []ServiceOption
		expectedService *Service
		wantErr         bool
		err             error
	}{
		{
			name: "positive case",
			opts: []ServiceOption{
				WithMsgChan(msgChan),
				WithCreateHandler(createHandler),
			},
			expectedService: &Service{
				msgChan:       msgChan,
				createHandler: createHandler,
			},
			wantErr: false,
		},
		{
			name: "negative case: msgChan is nil",
			opts: []ServiceOption{
				WithCreateHandler(createHandler),
			},
			wantErr: true,
			err:     errors.New("message channel is required"),
		},
		{
			name: "negative case: txHandler is nil",
			opts: []ServiceOption{
				WithMsgChan(msgChan),
			},
			wantErr: true,
			err:     errors.New("create handler is required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, err := New(tt.opts...)
			if tt.wantErr {
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
