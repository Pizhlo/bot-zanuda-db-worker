package operation

import (
	"context"
	"db-worker/internal/config/operation"
	interfaces "db-worker/internal/service/message/interface"
	"db-worker/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStorage struct {
}

func (m *mockStorage) Run(_ context.Context) error {
	return nil
}

func (m *mockStorage) Exec(_ context.Context) error {
	return nil
}

func (m *mockStorage) Stop(_ context.Context) error {
	return nil
}

type mockWorker struct {
}

func (m *mockWorker) Name() string {
	return "mockWorker"
}
func (m *mockWorker) Run(ctx context.Context) error {
	return nil
}
func (m *mockWorker) Stop(_ context.Context) error {
	return nil
}
func (m *mockWorker) MsgChan() chan interfaces.Message {
	return make(chan interfaces.Message)
}

//nolint:funlen // это тест
func TestNew(t *testing.T) {
	t.Parallel()

	msgChan := make(chan interfaces.Message)

	tests := []struct {
		name    string
		opts    []option
		wantErr require.ErrorAssertionFunc
		want    *Service
	}{
		{
			name: "positive case",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithConnection(&mockWorker{}),
				WithStorages([]storage.Driver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			want: &Service{
				cfg:        &operation.Operation{},
				connection: &mockWorker{},
				storages: []storage.Driver{
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
				WithConnection(&mockWorker{}),
				WithStorages([]storage.Driver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			want: &Service{
				cfg:        &operation.Operation{},
				connection: &mockWorker{},
				storages: []storage.Driver{
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
				WithStorages([]storage.Driver{
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
				WithConnection(&mockWorker{}),
				WithMsgChan(msgChan),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message channel is nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithConnection(&mockWorker{}),
				WithStorages([]storage.Driver{
					&mockStorage{},
				}),
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(tt.opts...)
			tt.wantErr(t, err)

			if got != nil {
				assert.ObjectsAreEqual(tt.want, got)
				assert.NotNil(t, got.quitChan)
			}
		})
	}
}

func TestStop(t *testing.T) {
	t.Parallel()

	op := &Service{
		quitChan: make(chan struct{}),
	}

	require.NoError(t, op.Stop(context.Background()))

	_, ok := <-op.quitChan
	assert.False(t, ok)
}
