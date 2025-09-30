package operation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    OperationConfig
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "valid operations",
			path: "./testdata/valid_operations.yaml",
			want: OperationConfig{
				Operations: []Operation{
					{
						Name: "create_notes",
						Type: OperationTypeCreate,
						Storages: []Storage{
							{
								Name:  "postgres_notes",
								Table: "notes.notes",
							},
						},
						Fields: []Field{
							{
								Name:     "user_id",
								Type:     FieldTypeInt64,
								Required: true,
							},
							{
								Name:     "text",
								Type:     FieldTypeString,
								Required: true,
							},
						},
						Request: Request{
							From: "rabbit_notes_create",
						},
					},
				},
				Connections: []Connection{
					{
						Name:       "rabbit_notes_create",
						Type:       ConnectionTypeRabbitMQ,
						Address:    "amqp://user:password@localhost:1234/",
						Queue:      "notes",
						RoutingKey: "create",
					},
				},
				Storages: []Storage{
					{
						Name: "postgres_notes",
						Type: StorageTypePostgres,
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name:    "invalid operations",
			path:    "./testdata/invalid_operations.yaml",
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			operation, err := LoadOperation(tt.path)
			tt.wantErr(t, err)
			require.Equal(t, tt.want, operation)
		})
	}
}
