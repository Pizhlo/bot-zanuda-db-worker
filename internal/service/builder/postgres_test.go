package builder

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithOperation(t *testing.T) {
	t.Parallel()

	builder := ForPostgres()

	require.NotNil(t, builder)
	assert.Equal(t, builder, &postgresBuilder{})

	operation := operation.Operation{Name: "test"}

	builder = builder.WithOperation(operation).(*postgresBuilder)
	require.NotNil(t, builder)
	assert.Equal(t, builder, &postgresBuilder{operation: operation})
}

func TestWithTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *postgresBuilder
		table   string
		want    *postgresBuilder
	}{
		{
			name:    "positive case",
			builder: &postgresBuilder{},
			table:   "test",
			want:    &postgresBuilder{table: "test"},
		},
		{
			name: "not nil builder",
			builder: &postgresBuilder{builder: &createPostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{},
			},
			},
			table: "test",
			want: &postgresBuilder{
				table: "test",
				builder: &createPostgresBuilder{
					basePostgresBuilder: basePostgresBuilder{
						table: "test",
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			builder = builder.WithTable(test.table).(*postgresBuilder)
			require.NotNil(t, builder)
			assert.Equal(t, test.want, builder)
		})
	}
}

func TestWithValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *postgresBuilder
		values  map[string]any
		want    *postgresBuilder
	}{
		{
			name:    "nil builder",
			builder: &postgresBuilder{},
			values:  map[string]any{"test": "test"},
			want:    &postgresBuilder{args: map[string]any{"test": "test"}},
		},
		{
			name: "not nil builder",
			builder: &postgresBuilder{builder: &createPostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{},
			},
			},
			values: map[string]any{"test": "test"},
			want: &postgresBuilder{
				args: map[string]any{"test": "test"},
				builder: &createPostgresBuilder{
					basePostgresBuilder: basePostgresBuilder{
						args: map[string]any{"test": "test"},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			builder = builder.WithValues(test.values).(*postgresBuilder)
			require.NotNil(t, builder)

			assert.Equal(t, test.want, builder)
		})
	}
}

func TestWithCreateOperation(t *testing.T) {
	t.Parallel()

	builder := &postgresBuilder{
		args:  map[string]any{"test": "test"},
		table: "test",
	}
	builder = builder.WithCreateOperation().(*postgresBuilder)
	require.NotNil(t, builder)

	assert.Equal(t, builder, &postgresBuilder{
		args:  map[string]any{"test": "test"},
		table: "test",
		builder: &createPostgresBuilder{
			basePostgresBuilder: basePostgresBuilder{
				args:  map[string]any{"test": "test"},
				table: "test",
			},
		}})
}

func TestBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *postgresBuilder
		want    *storage.Request
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "nil builder",
			builder: &postgresBuilder{},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "positive case",
			builder: &postgresBuilder{
				args:  map[string]any{"test": "test"},
				table: "test",
				builder: &createPostgresBuilder{
					basePostgresBuilder: basePostgresBuilder{
						args:  map[string]any{"test": "test"},
						table: "test",
					},
				},
			},
			want: &storage.Request{
				Val:  "INSERT INTO test (test) VALUES (?)",
				Args: []any{"test"},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			req, err := builder.Build()
			test.wantErr(t, err)
			assert.Equal(t, test.want, req)
		})
	}
}

func TestCreatePostgresBuilder_WithTable(t *testing.T) {
	t.Parallel()

	builder := &createPostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{},
	}

	builder.withTable("test")

	assert.Equal(t, builder, &createPostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{
			table: "test",
		},
	})
}

func TestCreatePostgresBuilder_WithValues(t *testing.T) {
	t.Parallel()

	builder := &createPostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{},
	}

	builder.withValues(map[string]any{"test": "test"})

	assert.Equal(t, builder, &createPostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{
			args: map[string]any{"test": "test"},
		},
	})
}

func TestCreatePostgresBuilder_Build(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *createPostgresBuilder
		want    *storage.Request
		wantErr require.ErrorAssertionFunc
	}{

		{
			name: "nil table",
			builder: &createPostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{
					table: "",
					args:  map[string]any{"test": "test"},
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "nil args",
			builder: &createPostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{
					table: "test",
					args:  nil,
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "positive case #1",
			builder: &createPostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{
					table: "test",
					args:  map[string]any{"test": "test"},
				},
			},
			want: &storage.Request{
				Val:  "INSERT INTO test (test) VALUES (?)",
				Args: []any{"test"},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req, err := test.builder.build()
			test.wantErr(t, err)
			assert.Equal(t, test.want, req)
		})
	}
}

func TestCollectColsAndVals(t *testing.T) {
	t.Parallel()

	args := map[string]any{"user_id": 1,
		"name":      "ivan ivanov",
		"email":     "ivan@ivanov.com",
		"age":       20,
		"is_active": true,
	}

	wantCols := []string{"user_id", "name", "email", "age", "is_active"}
	wantVals := []any{1, "ivan ivanov", "ivan@ivanov.com", 20, true}

	cols, vals := collectColsAndVals(args)
	assert.ElementsMatch(t, wantCols, cols)
	assert.ElementsMatch(t, wantVals, vals)
}
