package operation

import (
	"context"
	"db-worker/internal/config/operation"
	interfaces "db-worker/internal/service/message/interface"
	"db-worker/internal/service/worker/rabbit"
	"db-worker/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStorage struct {
}

func (m *mockStorage) Exec(_ context.Context) error {
	return nil
}

func (m *mockStorage) Close() error {
	return nil
}

func TestNew(t *testing.T) {
	t.Parallel()

	msgChan := make(chan interfaces.Message)

	tests := []struct {
		name    string
		opts    []option
		wantErr require.ErrorAssertionFunc
		want    *OperationService
	}{
		{
			name: "positive case",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithConnection(&rabbit.Worker{}),
				WithStorages([]storage.StorageDriver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			want: &OperationService{
				cfg:        &operation.Operation{},
				connection: &rabbit.Worker{},
				storages: []storage.StorageDriver{
					&mockStorage{},
				},
				msgChan:  msgChan,
				quitChan: make(chan struct{}),
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: cfg is nil",
			opts: []option{
				WithConnection(&rabbit.Worker{}),
				WithStorages([]storage.StorageDriver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			want: &OperationService{
				cfg:        &operation.Operation{},
				connection: &rabbit.Worker{},
				storages: []storage.StorageDriver{
					&mockStorage{},
				},
				msgChan:  msgChan,
				quitChan: make(chan struct{}),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: connection is nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithStorages([]storage.StorageDriver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storages are nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithConnection(&rabbit.Worker{}),
				WithMsgChan(msgChan),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message channel is nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithConnection(&rabbit.Worker{}),
				WithStorages([]storage.StorageDriver{
					&mockStorage{},
				}),
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.opts...)
			tt.wantErr(t, err)

			if got != nil {
				assert.ObjectsAreEqual(tt.want, got)
				assert.NotNil(t, got.quitChan)
			}
		})
	}
}

func TestClose(t *testing.T) {
	t.Parallel()

	op := &OperationService{
		quitChan: make(chan struct{}),
	}

	op.Close()

	_, ok := <-op.quitChan
	assert.False(t, ok)
}
