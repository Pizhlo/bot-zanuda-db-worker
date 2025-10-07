package operation

// Where - условие, по которому будет выполнена операция.
//   - Может объединять несколько полей: where (user_id = 10 and name = "test").
//   - Может объединять несколько условий: where (user_id = 10 and name = "test") and is_active = true.
//   - Может иметь только одно поле: where user_id = 10.
type Where struct {
	Type       WhereType    `yaml:"type" validate:"omitempty,oneof=and or not"`
	Fields     []WhereField `yaml:"fields" validate:"required,dive"`
	Conditions []Where      `yaml:"conditions" validate:"omitempty,dive"`
}

// WhereType - тип условия.
type WhereType string

const (
	// WhereTypeAnd - оператор AND.
	WhereTypeAnd WhereType = "and"
	// WhereTypeOr - оператор OR.
	WhereTypeOr WhereType = "or"
	// WhereTypeNot - оператор NOT.
	WhereTypeNot WhereType = "not"
)

// Operator - оператор сравнения.
type Operator string

const (
	// OperatorEqual - равенство.
	OperatorEqual Operator = "="
	// OperatorNotEqual - не равенство.
	OperatorNotEqual Operator = "!="
	// OperatorGreaterThan - больше.
	OperatorGreaterThan Operator = ">"
	// OperatorGreaterThanOrEqual - больше или равно.
	OperatorGreaterThanOrEqual Operator = ">="
	// OperatorLessThan - меньше.
	OperatorLessThan Operator = "<"
	// OperatorLessThanOrEqual - меньше или равно.
	OperatorLessThanOrEqual Operator = "<="
)

// WhereField - специальный тип для полей в условии where, расширяющий обычное поле Field.
type WhereField struct {
	Field    `yaml:",inline"`
	Value    any      `yaml:"value"`
	Operator Operator `yaml:"operator" validate:"oneof= = > < >= <= !="`
}

func (o *Operation) mapWhereFields(w Where) {
	for _, field := range w.Fields {
		o.WhereFieldsMap[field.Name] = field
	}

	for _, condition := range w.Conditions {
		o.mapWhereFields(condition)
	}
}
