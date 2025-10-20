package redis

import (
	"context"
	"db-worker/internal/config"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRedisClient struct {
	connectError error
	closeError   error

	wasConnected bool
	wasClosed    bool
}

func (m *mockRedisClient) Connect(_ context.Context) error {
	m.wasConnected = true

	return m.connectError
}

func (m *mockRedisClient) Close(_ context.Context) error {
	m.wasClosed = true

	return m.closeError
}

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []Option
		want    *Service
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			opts: []Option{
				WithCfg(&config.Redis{
					Type: config.RedisTypeSingle,
				}),
			},
			want: &Service{
				cfg: &config.Redis{
					Type: config.RedisTypeSingle,
				},
			},
			wantErr: require.NoError,
		},
		{
			name:    "negative case: cfg is nil",
			opts:    []Option{},
			want:    nil,
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(tt.opts...)
			tt.wantErr(t, err)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []Option
		service *Service
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			service: &Service{
				cfg: &config.Redis{
					Type: config.RedisTypeSingle,
				},
				client: &mockRedisClient{
					closeError: nil,
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: close error",
			service: &Service{
				cfg: &config.Redis{
					Type: config.RedisTypeSingle,
				},
				client: &mockRedisClient{
					closeError: errors.New("close error"),
				},
			},
			wantErr: require.Error,
		},
		{
			name: "positive case: no connection",
			service: &Service{
				cfg: &config.Redis{
					Type: config.RedisTypeSingle,
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.service.Stop(t.Context())
			tt.wantErr(t, err)

			if tt.service.client != nil {
				mockClient, ok := tt.service.client.(*mockRedisClient)
				require.True(t, ok, "client should be mockRedisClient")
				assert.True(t, mockClient.wasClosed)
			}
		})
	}
}
