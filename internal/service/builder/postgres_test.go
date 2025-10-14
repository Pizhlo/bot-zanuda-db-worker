package builder

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"testing"

	"github.com/huandu/go-sqlbuilder"
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

//nolint:funlen // тестовая функция
func TestWithUpdateOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *postgresBuilder
		want    Builder
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			builder: &postgresBuilder{
				args:  map[string]any{"test": "test"},
				table: "test",
				operation: operation.Operation{
					Name: "test",
					Type: operation.OperationTypeUpdate,
					Where: []operation.Where{
						{
							Fields: []operation.WhereField{{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							}},
						},
					},
					WhereFieldsMap: map[string]operation.WhereField{
						"test": {
							Field:    operation.Field{Name: "test"},
							Operator: operation.OperatorEqual,
							Value:    "test",
						},
					},
				},
			},
			want: &postgresBuilder{
				args:  map[string]any{"test": "test"},
				table: "test",
				operation: operation.Operation{
					Name: "test",
					Type: operation.OperationTypeUpdate,
					Where: []operation.Where{
						{
							Fields: []operation.WhereField{{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							}},
						},
					},
					WhereFieldsMap: map[string]operation.WhereField{
						"test": {
							Field:    operation.Field{Name: "test"},
							Operator: operation.OperatorEqual,
							Value:    "test",
						},
					},
				},
				builder: &updatePostgresBuilder{
					basePostgresBuilder: basePostgresBuilder{
						args:  map[string]any{"test": "test"},
						table: "test",
					},
					where: []operation.Where{
						{
							Fields: []operation.WhereField{{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							}},
						},
					},
					whereFieldsMap: map[string]operation.WhereField{
						"test": {
							Field:    operation.Field{Name: "test"},
							Operator: operation.OperatorEqual,
							Value:    "test",
						},
					},
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			b, err := builder.WithUpdateOperation()
			test.wantErr(t, err)

			assert.Equal(t, test.want, b)
		})
	}
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
				Val:  "INSERT INTO test (test) VALUES ($1)",
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
				Val:  "INSERT INTO test (test) VALUES ($1)",
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

func TestUpdatePostgresBuilder_WithTable(t *testing.T) {
	t.Parallel()

	builder := &updatePostgresBuilder{}
	builder.withTable("test")

	assert.Equal(t, "test", builder.table)
}

func TestUpdatePostgresBuilder_WithValues(t *testing.T) {
	t.Parallel()

	builder := &updatePostgresBuilder{}
	args := map[string]any{"test": "test"}
	builder.withValues(args)

	assert.Equal(t, args, builder.args)
}

//nolint:funlen // тестовая функция
func TestUpdatePostgresBuilder_Build(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *updatePostgresBuilder
		want    *storage.Request
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			builder: &updatePostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{
					table: "users.users",
					args:  map[string]any{"age": 20},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "name"},
								Operator: operation.OperatorEqual,
								Value:    "ivan",
							},
						},
					},
				},
				whereFieldsMap: map[string]operation.WhereField{
					"name": {
						Field:    operation.Field{Name: "name"},
						Operator: operation.OperatorEqual,
						Value:    "ivan",
					},
				},
				updateFieldsMap: map[string]operation.Field{
					"age": {
						Name:   "age",
						Update: true,
					},
				},
			},
			wantErr: require.NoError,
			want: &storage.Request{
				Val:  "UPDATE users.users SET age = $1 WHERE name = $2",
				Args: []any{20, "ivan"},
			},
		},
		{
			name: "nil table",
			builder: &updatePostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{
					table: "",
					args:  map[string]any{"test": "test"},
				},
			},
			wantErr: require.Error,
		},
		{
			name: "nil args",
			builder: &updatePostgresBuilder{
				basePostgresBuilder: basePostgresBuilder{
					table: "test",
					args:  nil,
				},
			},
			wantErr: require.Error,
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

func TestWhereBuilder_WithWhere(t *testing.T) {
	t.Parallel()

	where := []operation.Where{
		{
			Fields: []operation.WhereField{
				{
					Field:    operation.Field{Name: "test"},
					Operator: operation.OperatorEqual,
					Value:    "test",
				},
			},
		},
	}

	builder := &whereBuilder{}
	builder = builder.withWhere(where)

	assert.Equal(t, where, builder.where)
}

func TestNewWhereBuilder(t *testing.T) {
	t.Parallel()

	builder := newWhereBuilder()
	require.NotNil(t, builder)
}

func TestWhereBuilder_WithWhereFieldsMap(t *testing.T) {
	t.Parallel()

	whereFieldsMap := map[string]operation.WhereField{
		"test": {
			Field:    operation.Field{Name: "test"},
			Operator: operation.OperatorEqual,
			Value:    "test",
		},
	}

	builder := &whereBuilder{}
	builder = builder.withWhereFieldsMap(whereFieldsMap)

	assert.Equal(t, whereFieldsMap, builder.whereFieldsMap)
}

func TestWhereBuilder_WithTable(t *testing.T) {
	t.Parallel()

	builder := &whereBuilder{}
	builder = builder.withTable("test")

	assert.Equal(t, "test", builder.table)
}

func TestWhereBuilder_WithValues(t *testing.T) {
	t.Parallel()

	args := map[string]any{"test": "test"}

	builder := &whereBuilder{}
	builder = builder.withValues(args)

	assert.Equal(t, args, builder.args)
}

//nolint:funlen // тестовая функция
func TestWhereBuilder_InitUpdateBuilder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *whereBuilder
		want    *whereBuilder
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "test",
				args:  map[string]any{"test": "test"},
				whereFieldsMap: map[string]operation.WhereField{
					"test": {
						Field:    operation.Field{Name: "test"},
						Operator: operation.OperatorEqual,
						Value:    "test",
					},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							},
						},
					},
				},
			},
			want: &whereBuilder{
				table: "test",
				args:  map[string]any{"test": "test"},
				whereFieldsMap: map[string]operation.WhereField{
					"test": {
						Field:    operation.Field{Name: "test"},
						Operator: operation.OperatorEqual,
						Value:    "test",
					},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							},
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "table is empty",
			builder: &whereBuilder{
				args: map[string]any{"test": "test"},
				whereFieldsMap: map[string]operation.WhereField{
					"test": {
						Field:    operation.Field{Name: "test"},
						Operator: operation.OperatorEqual,
						Value:    "test",
					},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							},
						},
					},
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "args is nil",
			builder: &whereBuilder{
				table: "test",
				whereFieldsMap: map[string]operation.WhereField{
					"test": {
						Field:    operation.Field{Name: "test"},
						Operator: operation.OperatorEqual,
						Value:    "test",
					},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							},
						},
					},
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "whereFieldsMap is nil",
			builder: &whereBuilder{
				table: "test",
				args:  map[string]any{"test": "test"},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "test"},
								Operator: operation.OperatorEqual,
								Value:    "test",
							},
						},
					},
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			err := builder.initUpdateBuilder()
			test.wantErr(t, err)

			if test.want != nil {
				assert.Equal(t, test.want.table, builder.table)
				assert.Equal(t, test.want.args, builder.args)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestWhereBuilder_ApplyAssignments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *whereBuilder
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "test",
				args: map[string]any{
					"field1": "value1",
					"field2": "value2",
					"field3": "value3",
				},
				whereFieldsMap: map[string]operation.WhereField{
					"field3": {
						Field:    operation.Field{Name: "field3"},
						Operator: operation.OperatorEqual,
						Value:    "value3",
					},
				},
				updateFieldsMap: map[string]operation.Field{
					"field2": {
						Name:   "field2",
						Update: true,
					},
					"field1": {
						Name:   "field1",
						Update: true,
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "no fields to update",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "test",
				args: map[string]any{
					"field1": "value1",
				},
				whereFieldsMap: map[string]operation.WhereField{
					"field1": {
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			err := builder.applyAssignments()
			test.wantErr(t, err)
		})
	}
}

//nolint:funlen // тестовая функция
func TestWhereBuilder_Build(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *whereBuilder
		want    *storage.Request
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case #1",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "users.users",
				args: map[string]any{
					"age":  20,
					"name": "ivan",
				},
				whereFieldsMap: map[string]operation.WhereField{
					"name": {
						Field:    operation.Field{Name: "name"},
						Operator: operation.OperatorEqual,
						Value:    "ivan",
					},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "name"},
								Operator: operation.OperatorEqual,
								Value:    "ivan",
							},
						},
					},
				},
				updateFieldsMap: map[string]operation.Field{
					"age": {
						Name:   "age",
						Update: true,
					},
				},
			},
			want: &storage.Request{
				Val:  "UPDATE users.users SET age = $1 WHERE name = $2",
				Args: []any{20, "ivan"},
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case #2",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "users.users",
				args: map[string]any{
					"name":      "ivan",
					"email":     "ivan@ivanov.com",
					"user_id":   1,
					"is_active": true,
				},
				whereFieldsMap: map[string]operation.WhereField{
					"name": {
						Field:    operation.Field{Name: "name"},
						Operator: operation.OperatorEqual,
						Value:    "ivan",
					},
					"user_id": {
						Field:    operation.Field{Name: "user_id"},
						Operator: operation.OperatorEqual,
						Value:    1,
					},
					"is_active": {
						Field:    operation.Field{Name: "is_active"},
						Operator: operation.OperatorEqual,
						Value:    true,
					},
				},
				where: []operation.Where{
					{
						Type: operation.WhereTypeAnd,
						Conditions: []operation.Where{
							{
								Type: operation.WhereTypeAnd,
								Fields: []operation.WhereField{
									{
										Field:    operation.Field{Name: "user_id"},
										Operator: operation.OperatorEqual,
									},
									{
										Field:    operation.Field{Name: "name"},
										Operator: operation.OperatorEqual,
									},
								},
							},
							{
								Fields: []operation.WhereField{
									{
										Field:    operation.Field{Name: "is_active"},
										Operator: operation.OperatorEqual,
										Value:    true,
									},
								},
							},
						},
					},
				},
				updateFieldsMap: map[string]operation.Field{
					"email": {
						Name:   "email",
						Update: true,
					},
				},
			},
			want: &storage.Request{
				Val:  "UPDATE users.users SET email = $1 WHERE ((user_id = $2 AND name = $3) AND is_active = $4)",
				Args: []any{"ivan@ivanov.com", 1, "ivan", true},
			},
			wantErr: require.NoError,
		},
		{
			name: "error case: error init update builder",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "",
				args: map[string]any{
					"name": "ivan",
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "error case: error applyAssignments",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "users.users",
				args: map[string]any{
					"name": "ivan",
				},
				whereFieldsMap: map[string]operation.WhereField{
					"name": {
						Field:    operation.Field{Name: "name"},
						Operator: operation.OperatorEqual,
						Value:    "ivan",
					},
				},
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "name"},
								Operator: operation.OperatorEqual,
								Value:    "ivan",
							},
						},
					},
				},
			},
			want:    nil,
			wantErr: require.Error,
		},
		{
			name: "error case: error applyWhere",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "users.users",
				where: []operation.Where{},
				args: map[string]any{
					"name": "ivan",
				},
				whereFieldsMap: map[string]operation.WhereField{},
			},
			want:    nil,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			sql, args, err := builder.build()
			test.wantErr(t, err)

			if test.want != nil {
				assert.Equal(t, test.want.Val, sql)
				assert.Equal(t, test.want.Args, args)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestWhereBuilder_ApplyWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *whereBuilder
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case - single where condition",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "test",
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "field1"},
								Operator: operation.OperatorEqual,
								Value:    "value1",
							},
						},
					},
				},
				whereFieldsMap: map[string]operation.WhereField{
					"field1": {
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "empty where slice - len(b.where) == 0",
			builder: &whereBuilder{
				ub:             sqlbuilder.NewUpdateBuilder(),
				table:          "test",
				where:          []operation.Where{},
				whereFieldsMap: map[string]operation.WhereField{},
			},
			wantErr: require.NoError,
		},
		{
			name: "nil where slice - len(b.where) == 0",
			builder: &whereBuilder{
				ub:             sqlbuilder.NewUpdateBuilder(),
				table:          "test",
				where:          nil,
				whereFieldsMap: map[string]operation.WhereField{},
			},
			wantErr: require.NoError,
		},
		{
			name: "multiple where conditions - len(groupExprs) > 1",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "test",
				where: []operation.Where{
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "field1"},
								Operator: operation.OperatorEqual,
								Value:    "value1",
							},
						},
					},
					{
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "field2"},
								Operator: operation.OperatorEqual,
								Value:    "value2",
							},
						},
					},
				},
				whereFieldsMap: map[string]operation.WhereField{
					"field1": {
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
					"field2": {
						Field:    operation.Field{Name: "field2"},
						Operator: operation.OperatorEqual,
						Value:    "value2",
					},
				},
			},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			builder := test.builder
			builder.ub.SetFlavor(sqlbuilder.PostgreSQL)
			err := builder.applyWhere()
			test.wantErr(t, err)
		})
	}
}

//nolint:funlen // тестовая функция
func TestWhereBuilder_buildWhereExpr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder *whereBuilder
		where   operation.Where
		want    string
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "simple equal condition",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Fields: []operation.WhereField{
					{
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
				},
			},
			want:    `$1`,
			wantErr: require.NoError,
		},
		{
			name: "AND condition",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Type: operation.WhereTypeAnd,
				Fields: []operation.WhereField{
					{
						Field:    operation.Field{Name: "name"},
						Operator: operation.OperatorEqual,
						Value:    "ivan",
					},
					{
						Field:    operation.Field{Name: "age"},
						Operator: operation.OperatorGreaterThan,
						Value:    42,
					},
				},
			},
			want:    `($1 AND $2)`,
			wantErr: require.NoError,
		},
		{
			name: "OR condition",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Type: operation.WhereTypeOr,
				Fields: []operation.WhereField{
					{
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
					{
						Field:    operation.Field{Name: "field2"},
						Operator: operation.OperatorLessThan,
						Value:    100,
					},
				},
			},
			want:    `($1 OR $2)`,
			wantErr: require.NoError,
		},
		{
			name: "NOT condition",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Type: operation.WhereTypeNot,
				Fields: []operation.WhereField{
					{
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
				},
			},
			want:    `NOT ($1)`,
			wantErr: require.NoError,
		},
		{
			name: "nested conditions",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Type: operation.WhereTypeAnd,
				Fields: []operation.WhereField{
					{
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
				},
				Conditions: []operation.Where{
					{
						Type: operation.WhereTypeOr,
						Fields: []operation.WhereField{
							{
								Field:    operation.Field{Name: "field2"},
								Operator: operation.OperatorGreaterThan,
								Value:    42,
							},
							{
								Field:    operation.Field{Name: "field3"},
								Operator: operation.OperatorLessThan,
								Value:    100,
							},
						},
					},
				},
			},
			want:    `($1 AND ($2 OR $3))`,
			wantErr: require.NoError,
		},
		{
			name: "empty where",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Type:       operation.WhereTypeAnd,
				Fields:     []operation.WhereField{},
				Conditions: []operation.Where{},
			},
			want:    "",
			wantErr: require.Error,
		},
		{
			name: "unknown operator",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			where: operation.Where{
				Fields: []operation.WhereField{
					{
						Field:    operation.Field{Name: "field1"},
						Operator: "invalid",
						Value:    "value1",
					},
				},
			},
			want:    "",
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.builder.ub.SetFlavor(sqlbuilder.PostgreSQL)
			got, err := test.builder.buildWhereExpr(test.where)
			test.wantErr(t, err)

			if err == nil {
				require.Equal(t, test.want, got)
			}
		})
	}
}

//nolint:funlen // тестовая функция
func TestWhereBuilder_BuildComparator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		builder   *whereBuilder
		field     operation.WhereField
		wantValue string
		want      require.ComparisonAssertionFunc
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: value is not nil (OperatorEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorEqual,
				Value:    "ivan",
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is nil (OperatorEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorEqual,
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is not nil (OperatorNotEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorNotEqual,
				Value:    "ivan",
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is nil (OperatorNotEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorNotEqual,
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is not nil (OperatorGreaterThan)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorGreaterThan,
				Value:    "ivan",
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is nil (OperatorGreaterThan)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorGreaterThan,
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is not nil (OperatorGreaterThanOrEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorGreaterThanOrEqual,
				Value:    "ivan",
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is nil (OperatorGreaterThanOrEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorGreaterThanOrEqual,
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is not nil (OperatorLessThan)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorLessThan,
				Value:    "ivan",
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is nil (OperatorLessThan)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorLessThan,
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is not nil (OperatorLessThanOrEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorLessThanOrEqual,
				Value:    "ivan",
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "positive case: value is nil (OperatorLessThanOrEqual)",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorLessThanOrEqual,
			},
			wantValue: `$1`,
			want:      require.Equal,
			wantErr:   require.NoError,
		},
		{
			name: "error case: unknown operator",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
				args: map[string]any{
					"name": "ivan",
				},
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: "unknown",
			},
			wantValue: "",
			want:      require.Equal,
			wantErr:   require.Error,
		},
		{
			name: "error case: missing value",
			builder: &whereBuilder{
				ub: sqlbuilder.NewUpdateBuilder(),
			},
			field: operation.WhereField{
				Field:    operation.Field{Name: "name"},
				Operator: operation.OperatorEqual,
			},
			wantValue: "",
			want:      require.Equal,
			wantErr:   require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.builder.ub.SetFlavor(sqlbuilder.PostgreSQL)
			got, err := test.builder.buildComparator(test.field)
			test.wantErr(t, err)

			test.want(t, test.wantValue, got)
		})
	}
}
