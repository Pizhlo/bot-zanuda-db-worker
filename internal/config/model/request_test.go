package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  RequestConfig
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid rabbitmq config",
			config: RequestConfig{
				Connection: Connection{
					Type: "rabbitmq",
				},
				Config: map[string]any{
					"queue":       "test_queue",
					"routing_key": "test_key",
					"message": map[string]interface{}{
						"operation": map[any]any{
							"type":     fieldTypeString,
							"required": true,
							"value":    OperationTypeCreate,
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "invalid rabbitmq config",
			config: RequestConfig{
				Connection: Connection{
					Type: "rabbitmq",
				},
				Config: map[string]any{
					"queue":       "test_queue",
					"routing_key": "test_key",
					"message": map[string]interface{}{
						"operation": OperationTypeCreate,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "valid http config",
			config: RequestConfig{
				Connection: Connection{
					Type: "http",
				},
				Config: map[string]any{
					"url": "https://api.example.com/notes",
					"body": map[string]interface{}{
						"operation": map[any]any{
							"type":     fieldTypeString,
							"required": true,
							"value":    OperationTypeCreate,
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "invalid http config",
			config: RequestConfig{
				Connection: Connection{
					Type: "http",
				},
				Config: map[string]any{
					"body": map[string]interface{}{
						"operation": OperationTypeCreate,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid request type",
			config: RequestConfig{
				From:   "invalid",
				Config: map[string]interface{}{},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.wantErr(t, tt.config.Validate())
		})
	}
}

func TestGetRequestHandler(t *testing.T) {
	tests := []struct {
		name        string
		config      RequestConfig
		wantErr     require.ErrorAssertionFunc
		wantHandler RequestHandler
	}{
		{
			name: "valid rabbitmq config",
			config: RequestConfig{
				From: "rabbitmq",
				Config: map[string]interface{}{
					"queue":       "test_queue",
					"routing_key": "test_key",
					"message": map[string]interface{}{
						"operation": OperationTypeCreate,
					},
				},
				Connection: Connection{
					Type: "rabbitmq",
				},
			},
			wantErr: require.NoError,
			wantHandler: &RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": OperationTypeCreate,
				},
			},
		},
		{
			name: "valid http config",
			config: RequestConfig{
				From: "http",
				Config: map[string]interface{}{
					"url": "https://api.example.com/notes",
					"body": map[string]interface{}{
						"operation": OperationTypeCreate,
					},
				},
				Connection: Connection{
					Type: "http",
				},
			},
			wantErr: require.NoError,
			wantHandler: &HTTPRequest{
				URL: "https://api.example.com/notes",
				Body: map[string]interface{}{
					"operation": OperationTypeCreate,
				},
			},
		},
		{
			name: "invalid request type",
			config: RequestConfig{
				From:   "invalid",
				Config: map[string]interface{}{},
				Connection: Connection{
					Type: "invalid",
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, err := tt.config.GetRequestHandler()
			tt.wantErr(t, err)

			require.Equal(t, tt.wantHandler, handler)

			if tt.wantHandler != nil {
				require.Equal(t, tt.config.From, handler.GetType())
			}
		})
	}
}

func TestRabbitMQRequest_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request RabbitMQRequest
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid rabbitmq request",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"type":     fieldTypeString,
						"required": true,
						"value":    OperationTypeCreate,
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "missing queue",
			request: RabbitMQRequest{
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": OperationTypeCreate,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "missing routing key",
			request: RabbitMQRequest{
				Queue: "test_queue",
				Message: map[string]interface{}{
					"operation": OperationTypeCreate,
				},
			},
			wantErr: require.Error,
		},
		{
			name: "missing message",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
			},
			wantErr: require.Error,
		},
		{
			name: "missing operation field",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"some_field": "some_value",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "invalid operation type",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[string]any{
						"type":     fieldTypeInt,
						"required": true,
						"value":    OperationTypeCreate,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "operation's type is not string",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"type":     fieldTypeInt,
						"required": true,
						"value":    OperationTypeCreate,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "operation does not contain `type` field",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"required": true,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "operation does not contain `required` field",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"type":  fieldTypeString,
						"value": OperationTypeCreate,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "operation is not required",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"type":     fieldTypeString,
						"required": false,
						"value":    OperationTypeCreate,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "operation does not contain `value` field",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"type":     fieldTypeString,
						"required": true,
					},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "operation's value is not valid",
			request: RabbitMQRequest{
				Queue:      "test_queue",
				RoutingKey: "test_key",
				Message: map[string]interface{}{
					"operation": map[any]any{
						"type":     fieldTypeString,
						"required": true,
						"value":    "invalid",
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.wantErr(t, tt.request.Validate())
		})
	}
}

func TestHTTPRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request HTTPRequest
		wantErr bool
	}{
		{
			name: "valid http request",
			request: HTTPRequest{
				URL: "https://api.example.com/notes",
				Body: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: false,
		},
		{
			name: "missing url",
			request: HTTPRequest{
				Body: map[string]interface{}{
					"operation": "create",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRabbitMQRequest_GetType(t *testing.T) {
	request := RabbitMQRequest{}

	require.Equal(t, RabbitMQRequestType, request.GetType())
}

func TestHTTPRequest_GetType(t *testing.T) {
	request := HTTPRequest{}

	require.Equal(t, HTTPRequestType, request.GetType())
}

func TestRabbitMQRequest_GetTopic(t *testing.T) {
	request := RabbitMQRequest{
		Queue: "test_queue",
	}

	require.Equal(t, "test_queue", request.GetTopic())
}

func TestRabbitMQRequest_GetAddress(t *testing.T) {
	request := RabbitMQRequest{
		Address: "test_address",
	}

	require.Equal(t, "test_address", request.GetAddress())
}

func TestHTTPRequest_GetAddress(t *testing.T) {
	request := HTTPRequest{
		URL: "https://api.example.com/notes",
	}

	require.Equal(t, "https://api.example.com/notes", request.GetAddress())
}

func TestHTTPRequest_GetTopic(t *testing.T) {
	request := HTTPRequest{}

	require.Equal(t, "", request.GetTopic())
}
