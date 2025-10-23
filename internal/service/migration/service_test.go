package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMigrationLoader struct {
	loadError error
	wasLoaded bool
}

func (m *mockMigrationLoader) Load(ctx context.Context) error {
	m.wasLoaded = true
	return m.loadError
}

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []option
		want    *Service
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			opts: []option{
				WithMigrationLoader(&mockMigrationLoader{}),
			},
			want: &Service{
				loader: &mockMigrationLoader{},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: loader is nil",
			opts: []option{},
			want: nil,
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "loader is required")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(tt.opts...)
			tt.wantErr(t, err)

			if tt.want != nil {
				require.NotNil(t, got)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     *Service
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			svc: &Service{
				loader: &mockMigrationLoader{},
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: error loading migrations",
			svc: &Service{
				loader: &mockMigrationLoader{loadError: errors.New("error loading migrations")},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.svc.Run(t.Context())
			tt.wantErr(t, err)

			if tt.svc.loader != nil {
				assert.True(t, tt.svc.loader.(*mockMigrationLoader).wasLoaded)
			}
		})
	}
}

func TestStop(t *testing.T) {
	t.Parallel()

	svc := &Service{}

	require.NoError(t, svc.Stop(t.Context()))
}
