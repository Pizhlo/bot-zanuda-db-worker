package operation

import "fmt"

// Where - условие, по которому будет выполнена операция.
//   - Может объединять несколько полей: where (user_id = 10 and name = "test").
//   - Может объединять несколько условий: where (user_id = 10 and name = "test") and is_active = true.
//   - Может иметь только одно поле: where user_id = 10.
type Where struct {
	Type       WhereType    `yaml:"type" validate:"omitempty,oneof=and or not"`
	Fields     []WhereField `yaml:"fields" validate:"omitempty,dive"`
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

func (o *Operation) mapFieldsUpdate() {
	for _, field := range o.Fields {
		if field.Update {
			o.UpdateFieldsMap[field.Name] = field
		}
	}
}

// validateWhereFieldUpdate валидирует, что поля либо участвуют в обновлении, либо в условии where.
// WARNING: запускать после того, как отработали методы mapWhereFields и mapWhereFieldsUpdate.
func (o *Operation) validateWhereFieldUpdate(w Where) error {
	for _, field := range o.Fields {
		if _, ok := o.WhereFieldsMap[field.Name]; !ok && !field.Update {
			return fmt.Errorf("where field %q: not updated and not in where", field.Name)
		}
	}

	for _, condition := range w.Conditions {
		if err := o.validateWhereFieldUpdate(condition); err != nil {
			return err
		}
	}

	return nil
}
