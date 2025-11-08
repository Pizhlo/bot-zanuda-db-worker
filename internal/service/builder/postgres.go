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

func (b *postgresBuilder) WithDeleteOperation() (Builder, error) {
	b.builder = &deletePostgresBuilder{
		basePostgresBuilder: basePostgresBuilder{
			table: b.table,
			args:  b.args,
		},

		where:          b.operation.Where,
		whereFieldsMap: b.operation.WhereFieldsMap,
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

	wb := newWhereUpdateBuilder().withWhere(b.where).withWhereFieldsMap(b.whereFieldsMap).withUpdateFieldsMap(b.updateFieldsMap).withTable(b.table).withValues(b.args)

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

// whereUpdateBuilder - строитель запросов для where update операций в PostgreSQL.
type whereUpdateBuilder struct {
	ub              *sqlbuilder.UpdateBuilder
	table           string
	whereFieldsMap  map[string]operation.WhereField
	updateFieldsMap map[string]operation.Field
	where           []operation.Where
	args            map[string]any
}

func newWhereUpdateBuilder() *whereUpdateBuilder {
	return &whereUpdateBuilder{}
}

func (b *whereUpdateBuilder) withWhere(where []operation.Where) *whereUpdateBuilder {
	b.where = where

	return b
}

func (b *whereUpdateBuilder) withUpdateFieldsMap(updateFieldsMap map[string]operation.Field) *whereUpdateBuilder {
	b.updateFieldsMap = updateFieldsMap

	return b
}

func (b *whereUpdateBuilder) withWhereFieldsMap(whereFieldsMap map[string]operation.WhereField) *whereUpdateBuilder {
	b.whereFieldsMap = whereFieldsMap

	return b
}

func (b *whereUpdateBuilder) withTable(table string) *whereUpdateBuilder {
	b.table = table

	return b
}

func (b *whereUpdateBuilder) withValues(args map[string]any) *whereUpdateBuilder {
	b.args = args

	return b
}

func (b *whereUpdateBuilder) initUpdateBuilder() error {
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

func (b *whereUpdateBuilder) applyAssignments() error {
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

func (b *whereUpdateBuilder) build() (string, []any, error) {
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

//nolint:dupl // одинаковая реализация, но внутри разные типы билдеров
func (b *whereUpdateBuilder) applyWhere() error {
	if len(b.where) == 0 {
		return nil
	}

	groupExprs := make([]string, 0, len(b.where))
	for _, w := range b.where {
		expr, err := buildWhereExpr(w, b.ub, b.args)
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

type matcher interface {
	boolMatcher
	Equal(field string, value any) string
	NotEqual(field string, value any) string
	GreaterThan(field string, value any) string
	GreaterEqualThan(field string, value any) string
	LessThan(field string, value any) string
	LessEqualThan(field string, value any) string
}

type boolMatcher interface {
	Or(orExpr ...string) string
	Not(notExpr string) string
	And(andExpr ...string) string
}

func (b *whereUpdateBuilder) buildComparator(field operation.WhereField) (string, error) {
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

// deletePostgresBuilder - строитель запросов для delete операций в PostgreSQL.
type deletePostgresBuilder struct {
	basePostgresBuilder
	where          []operation.Where
	whereFieldsMap map[string]operation.WhereField
}

func (b *deletePostgresBuilder) withTable(table string) {
	b.table = table
}

func (b *deletePostgresBuilder) withValues(vals map[string]any) {
	b.args = vals
}

func (b *deletePostgresBuilder) build() (*storage.Request, error) {
	if b.table == "" {
		return nil, errors.New("table is nil")
	}

	if b.args == nil {
		return nil, errors.New("args is nil")
	}

	wb := newWhereDeleteBuilder().withWhere(b.where).withWhereFieldsMap(b.whereFieldsMap).withTable(b.table).withValues(b.args)

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

// whereDeleteBuilder - строитель запросов для where delete операций в PostgreSQL.
type whereDeleteBuilder struct {
	ub             *sqlbuilder.DeleteBuilder
	table          string
	whereFieldsMap map[string]operation.WhereField
	where          []operation.Where
	args           map[string]any
}

func newWhereDeleteBuilder() *whereDeleteBuilder {
	return &whereDeleteBuilder{}
}

func (b *whereDeleteBuilder) withWhere(where []operation.Where) *whereDeleteBuilder {
	b.where = where

	return b
}

func (b *whereDeleteBuilder) withWhereFieldsMap(whereFieldsMap map[string]operation.WhereField) *whereDeleteBuilder {
	b.whereFieldsMap = whereFieldsMap

	return b
}

func (b *whereDeleteBuilder) withTable(table string) *whereDeleteBuilder {
	b.table = table

	return b
}

func (b *whereDeleteBuilder) withValues(args map[string]any) *whereDeleteBuilder {
	b.args = args

	return b
}

func (b *whereDeleteBuilder) initDeleteBuilder() error {
	if b.table == "" {
		return errors.New("table is nil")
	}

	if b.args == nil {
		return errors.New("args is nil")
	}

	ub := sqlbuilder.NewDeleteBuilder()
	b.ub = ub
	ub.SetFlavor(sqlbuilder.PostgreSQL)
	ub.DeleteFrom(b.table)

	return nil
}

func (b *whereDeleteBuilder) build() (string, []any, error) {
	if err := b.initDeleteBuilder(); err != nil {
		return "", nil, err
	}

	if err := b.applyWhere(); err != nil {
		return "", nil, err
	}

	sql, args := b.ub.Build()

	return sql, args, nil
}

//nolint:dupl // одинаковая реализация, но внутри разные типы билдеров
func (b *whereDeleteBuilder) applyWhere() error {
	if len(b.where) == 0 {
		return nil
	}

	groupExprs := make([]string, 0, len(b.where))
	for _, w := range b.where {
		expr, err := buildWhereExpr(w, b.ub, b.args)
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

func buildWhereExpr(w operation.Where, matcher matcher, args map[string]any) (string, error) {
	parts := make([]string, 0, len(w.Fields)+len(w.Conditions))
	for _, f := range w.Fields {
		expr, err := buildComparator(matcher, f, args)
		if err != nil {
			return "", err
		}

		parts = append(parts, expr)
	}

	for _, c := range w.Conditions {
		expr, err := buildWhereExpr(c, matcher, args)
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
		return matcher.Or(parts...), nil
	case operation.WhereTypeNot:
		return matcher.Not(matcher.And(parts...)), nil
	default:
		if len(parts) == 1 {
			return parts[0], nil
		}

		return matcher.And(parts...), nil
	}
}

func buildComparator(matcher matcher, field operation.WhereField, args map[string]any) (string, error) {
	value := field.Value

	if value == nil {
		v, ok := args[field.Name]
		if !ok {
			return "", fmt.Errorf("missing value for where field %s", field.Name)
		}

		value = v
	}

	switch field.Operator {
	case operation.OperatorEqual:
		return matcher.Equal(field.Name, value), nil
	case operation.OperatorNotEqual:
		return matcher.NotEqual(field.Name, value), nil
	case operation.OperatorGreaterThan:
		return matcher.GreaterThan(field.Name, value), nil
	case operation.OperatorGreaterThanOrEqual:
		return matcher.GreaterEqualThan(field.Name, value), nil
	case operation.OperatorLessThan:
		return matcher.LessThan(field.Name, value), nil
	case operation.OperatorLessThanOrEqual:
		return matcher.LessEqualThan(field.Name, value), nil
	default:
		return "", fmt.Errorf("unknown operator: %s", field.Operator)
	}
}
