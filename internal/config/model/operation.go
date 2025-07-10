package config

import (
	"fmt"
	"strings"
)

// Operation представляет операцию над моделью
type Operation struct {
	Name            string           `yaml:"name" validate:"required"`                            // название операции
	Type            string           `yaml:"type" validate:"required,oneof=create update delete"` // тип операции
	Storage         string           `yaml:"storage" validate:"required,oneof=postgres"`          // хранилище, в котором нужно производить операцию
	Table           string           `yaml:"table" validate:"required"`                           // название таблицы, в которой будет храниться модель
	Fields          map[string]Field `yaml:"fields" validate:"required,validateModelFields"`      // поля, необходимые для операции
	WhereConditions map[string]Field `yaml:"where_conditions,omitempty"`                          // поля, по которым нужно производить обновление (where = ?)
	Constraints     []Constraint     `yaml:"constraints,omitempty"`                               // ограничения на уровне таблицы
	ValidationRules []ValidationRule `yaml:"validation_rules,omitempty"`                          // правила валидации на уровне операции
	Request         *RequestConfig   `yaml:"request,omitempty"`                                   // конфигурация запроса
}

// ValidationRule представляет правило валидации на уровне операции
type ValidationRule struct {
	Type  string `yaml:"type" validate:"required"`
	Field string `yaml:"field,omitempty"`
}

type SQLClause struct {
	SQL  string
	Args []interface{}
}

// BuildWhereClause строит WHERE условие для операции
func (o *Operation) BuildWhereClause(conditions map[string]interface{}) (SQLClause, error) {
	if len(o.WhereConditions) == 0 {
		return SQLClause{}, nil
	}

	var clauses []string
	var values []interface{}

	for fieldName, field := range o.WhereConditions {
		value, exists := conditions[fieldName]
		if !exists && field.Required {
			return SQLClause{}, fmt.Errorf("required where condition field missing: %s", fieldName)
		}

		if exists {
			if err := field.ValidateField(value); err != nil {
				return SQLClause{}, fmt.Errorf("invalid where condition field %s: %w", fieldName, err)
			}

			clauses = append(clauses, fmt.Sprintf("%s = ?", fieldName))
			values = append(values, value)
		}
	}

	return SQLClause{
		SQL:  strings.Join(clauses, " AND "),
		Args: values,
	}, nil
}

// Validate выполняет бизнес-валидацию операции
func (o *Operation) Validate() error {
	reqIDField, ok := o.Fields["request_id"]
	if !ok {
		return fmt.Errorf("missing required field 'request_id' in operation's fields")
	}

	if reqIDField.Type != "uuid" {
		return fmt.Errorf("field 'request_id' must have type 'uuid', got '%s'", reqIDField.Type)
	}

	if !reqIDField.Required {
		return fmt.Errorf("field 'request_id' must be required")
	}

	if o.Request != nil {
		if err := o.Request.Validate(); err != nil {
			return fmt.Errorf("invalid request config: %w", err)
		}
	} else {
		return fmt.Errorf("request config is required")
	}

	if o.WhereConditions != nil {
		for fieldName := range o.WhereConditions {
			if _, ok := o.Fields[fieldName]; !ok {
				return fmt.Errorf("field '%s' is not defined in fields", fieldName)
			}
		}
	}

	return nil
}
