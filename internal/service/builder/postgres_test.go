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
		{
			name: "where is nil",
			builder: &postgresBuilder{
				args:  map[string]any{"test": "test"},
				table: "test",
				operation: operation.Operation{
					Name: "test",
					Type: operation.OperationTypeUpdate,
					WhereFieldsMap: map[string]operation.WhereField{
						"test": {
							Field:    operation.Field{Name: "test"},
							Operator: operation.OperatorEqual,
							Value:    "test",
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
				ub:    sqlbuilder.NewUpdateBuilder().Update("test"),
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
				ub:   sqlbuilder.NewUpdateBuilder(),
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
				ub:    sqlbuilder.NewUpdateBuilder(),
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
				ub:    sqlbuilder.NewUpdateBuilder(),
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
		{
			name: "where is nil",
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

func TestApplyAssignments(t *testing.T) {
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

func TestApplyWhere(t *testing.T) {
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
			name: "empty where",
			builder: &whereBuilder{
				ub:    sqlbuilder.NewUpdateBuilder(),
				table: "test",
				where: []operation.Where{},
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
			want:    `"field1" = $1`,
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
						Field:    operation.Field{Name: "field1"},
						Operator: operation.OperatorEqual,
						Value:    "value1",
					},
					{
						Field:    operation.Field{Name: "field2"},
						Operator: operation.OperatorGreaterThan,
						Value:    42,
					},
				},
			},
			want:    `("field1" = $1 AND "field2" > $2)`,
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
			want:    `("field1" = $1 OR "field2" < $2)`,
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
			want:    `NOT ("field1" = $1)`,
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
			want:    `("field1" = $1 AND ("field2" > $2 OR "field3" < $3))`,
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
