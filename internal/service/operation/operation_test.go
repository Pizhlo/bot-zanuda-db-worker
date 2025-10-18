package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockUnitOfWork struct {
	buildRequest bool
	execRequests bool

	execChan chan struct{} // сообщение о том, что запросы выполнены

	execError  error
	buildError error

	mu sync.Mutex
}

func (m *mockUnitOfWork) BuildRequests(msg map[string]interface{}) (map[storage.Driver]*storage.Request, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.buildRequest = true

	return nil, m.buildError
}

func (m *mockUnitOfWork) ExecRequests(ctx context.Context, requests map[storage.Driver]*storage.Request) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.execChan != nil {
		m.execChan <- struct{}{}
	}

	m.execRequests = true

	return m.execError
}

func (m *mockUnitOfWork) getBuildRequest() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.buildRequest
}

func (m *mockUnitOfWork) getExecRequests() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.execRequests
}

//nolint:funlen // это тест
func TestNew(t *testing.T) {
	t.Parallel()

	uow := &mockUnitOfWork{}

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
				WithMsgChan(msgChan),
				WithUow(uow),
			},
			want: &Service{
				cfg:      &operation.Operation{},
				msgChan:  msgChan,
				quitChan: make(chan struct{}),
				uow:      uow,
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: cfg is nil",
			opts: []option{
				WithMsgChan(msgChan),
			},
			want: &Service{
				cfg:      &operation.Operation{},
				msgChan:  msgChan,
				quitChan: make(chan struct{}),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: message channel is nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithUow(uow),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: uow is nil",
			opts: []option{
				WithCfg(&operation.Operation{}),
				WithMsgChan(msgChan),
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
				// Сравниваем поля по отдельности, исключая каналы
				assert.Equal(t, tt.want.cfg, got.cfg)
				assert.Equal(t, tt.want.msgChan, got.msgChan)
				assert.Equal(t, tt.want.uow, got.uow)
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
