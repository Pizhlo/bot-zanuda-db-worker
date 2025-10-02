package rabbit

import (
	interfaces "db-worker/internal/service/message/interface"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []Option
		want    *Worker
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			opts: []Option{
				WithName("test"),
				WithAddress("test"),
				WithExchange("test"),
				WithRoutingKey("test"),
				WithInsertTimeout(1),
				WithReadTimeout(1),
			},
			want: &Worker{
				config: struct {
					address    string
					name       string
					exchange   string
					routingKey string
				}{
					address:    "test",
					name:       "test",
					exchange:   "test",
					routingKey: "test",
				},
				msgChan: make(chan interfaces.Message),
				queue: amqp.Queue{
					Name: "test",
				},
				conn:          &amqp.Connection{},
				channel:       &amqp.Channel{},
				insertTimeout: 1,
				readTimeout:   1,
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: read timeout is 0",
			opts: []Option{
				WithName("test"),
				WithAddress("test"),
				WithExchange("test"),
				WithRoutingKey("test"),
				WithInsertTimeout(1),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: insert timeout is 0",
			opts: []Option{
				WithName("test"),
				WithAddress("test"),
				WithExchange("test"),
				WithRoutingKey("test"),
				WithReadTimeout(1),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: name is empty",
			opts: []Option{
				WithAddress("test"),
				WithExchange("test"),
				WithRoutingKey("test"),
				WithInsertTimeout(1),
				WithReadTimeout(1),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: address is empty",
			opts: []Option{
				WithName("test"),
				WithExchange("test"),
				WithRoutingKey("test"),
				WithInsertTimeout(1),
				WithReadTimeout(1),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: exchange is empty",
			opts: []Option{
				WithName("test"),
				WithAddress("test"),
				WithRoutingKey("test"),
				WithInsertTimeout(1),
				WithReadTimeout(1),
			},
			wantErr: require.Error,
		},
		{
			name: "negative case: routing key is empty",
			opts: []Option{
				WithName("test"),
				WithAddress("test"),
				WithExchange("test"),
				WithInsertTimeout(1),
				WithReadTimeout(1),
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
				assert.NotNil(t, got.msgChan)
				assert.NotNil(t, got.quitChan)
			}
		})
	}
}

func TestName(t *testing.T) {
	t.Parallel()

	rabbit := &Worker{
		config: struct {
			address    string
			name       string
			exchange   string
			routingKey string
		}{
			name: "test",
		},
	}

	require.Equal(t, "test", rabbit.Name())
}

func TestMsgChan(t *testing.T) {
	t.Parallel()

	rabbit := &Worker{
		msgChan: make(chan interfaces.Message),
	}

	require.Equal(t, rabbit.msgChan, rabbit.MsgChan())
}
