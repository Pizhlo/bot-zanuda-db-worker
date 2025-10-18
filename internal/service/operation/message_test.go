package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadMessages(t *testing.T) {
	t.Parallel()

	msgChan := make(chan map[string]interface{})
	quitChan := make(chan struct{})

	mockUow := &mockUnitOfWork{
		execChan: make(chan struct{}),
	}

	tests := []struct {
		name string
		svc  *Service
		msg  map[string]interface{}
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
				uow:      mockUow,
				msgChan:  msgChan,
				quitChan: quitChan,
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go tt.svc.readMessages(ctx)

			tt.svc.msgChan <- tt.msg

			close(tt.svc.quitChan)

			select {
			case <-mockUow.execChan:
				assert.True(t, mockUow.getBuildRequest())
				assert.True(t, mockUow.getExecRequests())
			case <-time.After(1 * time.Second):
				assert.Fail(t, "timeout")
			}
		})
	}
}

//nolint:funlen // много тест-кейсов
func TestProcessMessage(t *testing.T) {
	t.Parallel()

	execChan := make(chan struct{})

	tests := []struct {
		name    string
		svc     *Service
		msg     map[string]interface{}
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
				uow: &mockUnitOfWork{
					execChan: execChan,
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: error validate message",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
						{
							Name:     "field2",
							Type:     "string",
							Required: true,
						},
					},
				},
				uow: &mockUnitOfWork{
					execChan: make(chan struct{}),
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: error build requests",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
				uow: &mockUnitOfWork{
					execChan:   execChan,
					buildError: errors.New("error build requests"),
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: error exec requests",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
				},
				uow: &mockUnitOfWork{
					execChan:  execChan,
					execError: errors.New("error build requests"),
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go func() {
				<-execChan
			}()

			err := tt.svc.processMessage(ctx, tt.msg)
			tt.wantErr(t, err)
		})
	}
}
