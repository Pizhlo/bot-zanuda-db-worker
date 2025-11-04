package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []Option
		want *Service
	}{
		{
			name: "positive case: default options",
			opts: []Option{
				WithRegisterer(prometheus.NewRegistry()),
			},
			want: &Service{
				namespace: "dbworker",
				subsystem: "core",
			},
		},
		{
			name: "positive case: custom options",
			opts: []Option{
				WithRegisterer(prometheus.NewRegistry()),
				WithNamespace("test"),
				WithSubsystem("test"),
			},
			want: &Service{
				namespace: "test",
				subsystem: "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := New(tt.opts...)

			assert.Equal(t, tt.want.namespace, got.namespace)
			assert.Equal(t, tt.want.subsystem, got.subsystem)
			assert.NotNil(t, got.registry)
		})
	}
}
