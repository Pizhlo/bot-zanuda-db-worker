package model

import (
	"context"
	"db-worker/internal/config"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRequestProcessor_ProcessRequest(t *testing.T) {
	processor := NewRequestProcessor()

	// Регистрируем обработчики
	processor.RegisterHandler(NewRabbitMQProcessor())
	processor.RegisterHandler(NewHTTPProcessor())

	tests := []struct {
		name    string
		config  *config.RequestConfig
		wantErr bool
	}{
		{
			name: "valid rabbitmq request",
			config: &config.RequestConfig{
				From: "rabbitmq",
				Config: map[string]interface{}{
					"queue":       "test_queue",
					"routing_key": "test_key",
					"message": map[string]interface{}{
						"operation": "create",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid http request",
			config: &config.RequestConfig{
				From: "http",
				Config: map[string]interface{}{
					"url": "https://api.example.com/notes",
					"body": map[string]interface{}{
						"operation": "create",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "unsupported request type",
			config: &config.RequestConfig{
				From:   "unsupported",
				Config: map[string]interface{}{},
			},
			wantErr: true,
		},
		{
			name: "invalid rabbitmq config",
			config: &config.RequestConfig{
				From:   "rabbitmq",
				Config: map[string]interface{}{
					// Отсутствуют обязательные поля
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := processor.ProcessRequest(ctx, tt.config)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRabbitMQProcessor_Process(t *testing.T) {
	processor := NewRabbitMQProcessor()

	validHandler := &config.RabbitMQRequest{
		Queue:      "test_queue",
		RoutingKey: "test_key",
		Message: map[string]interface{}{
			"operation": "create",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := processor.Process(ctx, validHandler)
	require.NoError(t, err)
}

func TestHTTPProcessor_Process(t *testing.T) {
	processor := NewHTTPProcessor()

	validHandler := &config.HTTPRequest{
		URL: "https://api.example.com/notes",
		Body: map[string]interface{}{
			"operation": "create",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := processor.Process(ctx, validHandler)
	require.NoError(t, err)
}

func TestRequestProcessor_RegisterHandler(t *testing.T) {
	processor := NewRequestProcessor()

	// Проверяем, что обработчики не зарегистрированы
	_, exists := processor.handlers["rabbitmq"]
	require.False(t, exists)

	_, exists = processor.handlers["http"]
	require.False(t, exists)

	// Регистрируем обработчики
	processor.RegisterHandler(NewRabbitMQProcessor())
	processor.RegisterHandler(NewHTTPProcessor())

	// Проверяем, что обработчики зарегистрированы
	_, exists = processor.handlers["rabbitmq"]
	require.True(t, exists)

	_, exists = processor.handlers["http"]
	require.True(t, exists)
}
