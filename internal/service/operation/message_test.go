package operation

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"testing"
)

func TestReadMessages(t *testing.T) {
	t.Parallel()

	msgChan := make(chan map[string]interface{})
	quitChan := make(chan struct{})

	tests := []struct {
		name string
		svc  *Service
		msg  map[string]interface{}
	}{
		{
			name: "positive case",
			svc: &Service{
				cfg: &operation.Operation{
					Name: "test",
					Fields: []operation.Field{
						{
							Name: "field1",
							Type: "string",
						},
					},
				},
				storages: []storage.Driver{
					&mockStorage{},
				},
				msgChan:  msgChan,
				quitChan: quitChan,
				mapFields: map[string]operation.Field{
					"field1": {
						Name: "field1",
						Type: "string",
					},
				},
			},
			msg: map[string]interface{}{
				"field1": "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go tt.svc.readMessages(ctx)

			tt.svc.msgChan <- tt.msg

			close(tt.svc.quitChan)
		})
	}
}
