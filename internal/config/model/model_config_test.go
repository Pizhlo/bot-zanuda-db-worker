package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadModelConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{
			name:    "valid model config",
			file:    "../testdata/valid_model.yaml",
			wantErr: false,
		},
		{
			name:    "invalid model config",
			file:    "../testdata/invalid_model.yaml",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config, err := LoadModelConfig(tt.file)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
			require.NotEmpty(t, config.Models)
		})
	}
}
