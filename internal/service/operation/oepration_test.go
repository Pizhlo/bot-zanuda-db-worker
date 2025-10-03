package operation

import (
	"context"
	"db-worker/internal/config/operation"
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

//nolint:funlen // это тест
func TestNew(t *testing.T) {
	t.Parallel()

	msgChan := make(chan map[string]interface{})

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
				WithStorages([]storage.Driver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			want: &Service{
				cfg: &operation.Operation{},
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
				WithStorages([]storage.Driver{
					&mockStorage{},
				}),
				WithMsgChan(msgChan),
			},
			want: &Service{
				cfg: &operation.Operation{},
				storages: []storage.Driver{
					&mockStorage{},
				},
				msgChan:  msgChan,
				quitChan: make(chan struct{}),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: storages are nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithMsgChan(msgChan),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message channel is nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
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
		cfg: &operation.Operation{
			Name: "test",
		},
	}

	require.NoError(t, op.Stop(context.Background()))

	_, ok := <-op.quitChan
	assert.False(t, ok)
}
