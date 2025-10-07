package builder

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"errors"

	"github.com/huandu/go-sqlbuilder"
)

// builder - интерфейс для создания запросов.
type builder interface {
	build() (*storage.Request, error)
	withTable(table string)
	withValues(vals map[string]any)
}

type postgresBuilder struct {
	operation operation.Operation
	builder   builder
	table     string
	args      map[string]any
}

func (b *postgresBuilder) WithOperation(operation operation.Operation) Builder {
	b.operation = operation

	return b
}

// WithTable устанавливает название таблицы для драйвера PostgreSQL.
func (b *postgresBuilder) WithTable(table string) Builder {
	b.table = table

	if b.builder != nil {
		b.builder.withTable(table)
	}

	return b
}

func (b *postgresBuilder) WithValues(vals map[string]any) Builder {
	b.args = vals

	if b.builder != nil {
		b.builder.withValues(vals)
	}

	return b
}

func (b *postgresBuilder) WithCreateOperation() Builder {
	b.builder = &createPostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{
			table: b.table,
			args:  b.args,
		},
	}

	return b
}

func (b *postgresBuilder) Build() (*storage.Request, error) {
	if b.builder == nil {
		return nil, errors.New("builder is nil")
	}

	return b.builder.build()
}

// basePostgresBuilder - базовый строитель запросов для PostgreSQL.
// Содержит в себе базовые поля для всех запросов.
type basePostgresBuilder struct {
	table string
	args  map[string]any
}

// createPostgresBuilder - строитель запросов для insert операций в PostgreSQL.
type createPostgresBuilder struct {
	basePostgresBuilder
}

func (b *createPostgresBuilder) withTable(table string) {
	b.table = table
}

func (b *createPostgresBuilder) withValues(vals map[string]any) {
	b.args = vals
}

func (b *createPostgresBuilder) build() (*storage.Request, error) {
	if b.table == "" {
		return nil, errors.New("table is nil")
	}

	if b.args == nil {
		return nil, errors.New("args is nil")
	}

	sb := sqlbuilder.NewInsertBuilder() // In common scenarios, it is necessary to escape all user inputs. To achieve this, initialize a builder at the outset.

	sb.InsertInto(b.table)

	cols, vals := collectColsAndVals(b.args)

	sb.Cols(cols...)
	sb.Values(vals...)

	return &storage.Request{
		Val:  sb.String(),
		Args: vals,
	}, nil
}

func collectColsAndVals(args map[string]any) ([]string, []any) {
	cols := make([]string, 0, len(args))
	vals := make([]any, 0, len(args))

	for name, value := range args {
		cols = append(cols, name)
		vals = append(vals, value)
	}

	return cols, vals
}
