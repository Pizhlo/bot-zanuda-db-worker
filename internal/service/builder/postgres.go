package builder

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"errors"
	"fmt"

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

func (b *postgresBuilder) WithUpdateOperation() (Builder, error) {
	b.builder = &updatePostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{
			table: b.table,
			args:  b.args,
		},

		where:           b.operation.Where,
		whereFieldsMap:  b.operation.WhereFieldsMap,
		updateFieldsMap: b.operation.UpdateFieldsMap,
	}

	return b, nil
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

	sb.SetFlavor(sqlbuilder.PostgreSQL)

	sb.InsertInto(b.table)

	cols, vals := collectColsAndVals(b.args)

	sb.Cols(cols...)
	sb.Values(vals...)

	sql, args := sb.Build()

	return &storage.Request{
		Val:  sql,
		Args: args,
		Raw:  b.args,
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

// updatePostgresBuilder - строитель запросов для update операций в PostgreSQL.
type updatePostgresBuilder struct {
	basePostgresBuilder
	where           []operation.Where
	whereFieldsMap  map[string]operation.WhereField
	updateFieldsMap map[string]operation.Field
}

func (b *updatePostgresBuilder) withTable(table string) {
	b.table = table
}

func (b *updatePostgresBuilder) withValues(vals map[string]any) {
	b.args = vals
}

func (b *updatePostgresBuilder) build() (*storage.Request, error) {
	if b.table == "" {
		return nil, errors.New("table is nil")
	}

	if b.args == nil {
		return nil, errors.New("args is nil")
	}

	wb := newWhereBuilder().withWhere(b.where).withWhereFieldsMap(b.whereFieldsMap).withUpdateFieldsMap(b.updateFieldsMap).withTable(b.table).withValues(b.args)

	sql, args, err := wb.build()
	if err != nil {
		return nil, err
	}

	return &storage.Request{
		Val:  sql,
		Args: args,
		Raw:  b.args,
	}, nil
}

// whereBuilder - строитель запросов для where операций в PostgreSQL.
type whereBuilder struct {
	ub              *sqlbuilder.UpdateBuilder
	table           string
	whereFieldsMap  map[string]operation.WhereField
	updateFieldsMap map[string]operation.Field
	where           []operation.Where
	args            map[string]any
}

func newWhereBuilder() *whereBuilder {
	return &whereBuilder{}
}

func (b *whereBuilder) withWhere(where []operation.Where) *whereBuilder {
	b.where = where

	return b
}

func (b *whereBuilder) withUpdateFieldsMap(updateFieldsMap map[string]operation.Field) *whereBuilder {
	b.updateFieldsMap = updateFieldsMap

	return b
}

func (b *whereBuilder) withWhereFieldsMap(whereFieldsMap map[string]operation.WhereField) *whereBuilder {
	b.whereFieldsMap = whereFieldsMap

	return b
}

func (b *whereBuilder) withTable(table string) *whereBuilder {
	b.table = table

	return b
}

func (b *whereBuilder) withValues(args map[string]any) *whereBuilder {
	b.args = args

	return b
}

func (b *whereBuilder) initUpdateBuilder() error {
	if b.table == "" {
		return errors.New("table is nil")
	}

	if b.args == nil {
		return errors.New("args is nil")
	}

	ub := sqlbuilder.NewUpdateBuilder()
	b.ub = ub
	ub.SetFlavor(sqlbuilder.PostgreSQL)
	ub.Update(b.table)

	return nil
}

func (b *whereBuilder) applyAssignments() error {
	assignments := make([]string, 0, len(b.args))
	for name := range b.updateFieldsMap {
		value, ok := b.args[name]
		if !ok {
			return fmt.Errorf("missing value for update field %s", name)
		}

		assignments = append(assignments, b.ub.Assign(name, value))
	}

	if len(assignments) > 0 {
		b.ub.Set(assignments...)
	}

	if b.ub.NumAssignment() == 0 {
		return errors.New("no fields to update")
	}

	return nil
}

func (b *whereBuilder) build() (string, []any, error) {
	if err := b.initUpdateBuilder(); err != nil {
		return "", nil, err
	}

	// нужно проставить все поля, которых нет в where
	if err := b.applyAssignments(); err != nil {
		return "", nil, err
	}

	if err := b.applyWhere(); err != nil {
		return "", nil, err
	}

	sql, args := b.ub.Build()

	return sql, args, nil
}

func (b *whereBuilder) applyWhere() error {
	if len(b.where) == 0 {
		return nil
	}

	groupExprs := make([]string, 0, len(b.where))
	for _, w := range b.where {
		expr, err := b.buildWhereExpr(w)
		if err != nil {
			return err
		}

		if expr != "" {
			groupExprs = append(groupExprs, expr)
		}
	}

	switch len(groupExprs) {
	case 0:
		// невозможно, т.к. в buildWhereExpr есть проверка на пустоту
		return nil
	case 1:
		b.ub.Where(groupExprs[0])
		return nil
	default:
		b.ub.Where(b.ub.And(groupExprs...))
		return nil
	}
}

func (b *whereBuilder) buildWhereExpr(w operation.Where) (string, error) {
	parts := make([]string, 0, len(w.Fields)+len(w.Conditions))
	for _, f := range w.Fields {
		expr, err := b.buildComparator(f)
		if err != nil {
			return "", err
		}

		parts = append(parts, expr)
	}

	for _, c := range w.Conditions {
		expr, err := b.buildWhereExpr(c)
		if err != nil {
			return "", err
		}

		if expr != "" {
			parts = append(parts, expr)
		}
	}

	if len(parts) == 0 {
		return "", fmt.Errorf("parts is nil")
	}

	switch w.Type {
	case operation.WhereTypeOr:
		return b.ub.Or(parts...), nil
	case operation.WhereTypeNot:
		return b.ub.Not(b.ub.And(parts...)), nil
	default:
		if len(parts) == 1 {
			return parts[0], nil
		}

		return b.ub.And(parts...), nil
	}
}

func (b *whereBuilder) buildComparator(field operation.WhereField) (string, error) {
	value := field.Value

	if value == nil {
		v, ok := b.args[field.Name]
		if !ok {
			return "", fmt.Errorf("missing value for where field %s", field.Name)
		}

		value = v
	}

	switch field.Operator {
	case operation.OperatorEqual:
		return b.ub.Equal(field.Name, value), nil
	case operation.OperatorNotEqual:
		return b.ub.NotEqual(field.Name, value), nil
	case operation.OperatorGreaterThan:
		return b.ub.GreaterThan(field.Name, value), nil
	case operation.OperatorGreaterThanOrEqual:
		return b.ub.GreaterEqualThan(field.Name, value), nil
	case operation.OperatorLessThan:
		return b.ub.LessThan(field.Name, value), nil
	case operation.OperatorLessThanOrEqual:
		return b.ub.LessEqualThan(field.Name, value), nil
	default:
		return "", fmt.Errorf("unknown operator: %s", field.Operator)
	}
}
