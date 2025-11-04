package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	customRegistry := prometheus.NewRegistry()

	tests := []struct {
		name string
		opts []Option
		want *Service
	}{
		{
			name: "positive case: default options",
			opts: []Option{},
			want: &Service{
				registry:  prometheus.DefaultRegisterer,
				namespace: "dbworker",
				subsystem: "core",
			},
		},
		{
			name: "positive case: custom options",
			opts: []Option{
				WithRegisterer(customRegistry),
				WithNamespace("test"),
				WithSubsystem("test"),
			},
			want: &Service{
				registry:  customRegistry,
				namespace: "test",
				subsystem: "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := New(tt.opts...)
			require.Equal(t, tt.want, got)
		})
	}
}
