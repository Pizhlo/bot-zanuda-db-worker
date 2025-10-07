package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapWhereFields(t *testing.T) {
	t.Parallel()

	op := Operation{
		WhereFieldsMap: make(map[string]WhereField),
		Fields: []Field{
			{Name: "name", Type: FieldTypeString, Update: true},
			{Name: "age", Type: FieldTypeInt64, Update: true},
			{Name: "email", Type: FieldTypeString, Update: true},
			{Name: "is_active", Type: FieldTypeBool, Update: true},
		},
		Where: []Where{
			{
				Fields: []WhereField{
					{
						Field:    Field{Name: "name", Type: FieldTypeString, Update: true},
						Operator: OperatorEqual,
						Value:    "John Doe",
					},
					{
						Field:    Field{Name: "is_active", Type: FieldTypeBool, Update: true},
						Operator: OperatorEqual,
						Value:    true,
					},
				},
				Conditions: []Where{
					{
						Fields: []WhereField{
							{Field: Field{Name: "age", Type: FieldTypeInt64, Update: true}, Operator: OperatorEqual, Value: 30},
						},
					},
				},
			},
		},
	}

	for _, where := range op.Where {
		op.mapWhereFields(where)
	}

	expectedMap := map[string]WhereField{
		"name":      {Field: Field{Name: "name", Type: FieldTypeString, Update: true}, Operator: OperatorEqual, Value: "John Doe"},
		"is_active": {Field: Field{Name: "is_active", Type: FieldTypeBool, Update: true}, Operator: OperatorEqual, Value: true},
		"age":       {Field: Field{Name: "age", Type: FieldTypeInt64, Update: true}, Operator: OperatorEqual, Value: 30},
	}

	assert.Equal(t, op.WhereFieldsMap, expectedMap)
}

func TestMapFieldsUpdate(t *testing.T) {
	t.Parallel()

	op := Operation{
		Fields: []Field{
			{Name: "name", Type: FieldTypeString, Update: true},
			{Name: "age", Type: FieldTypeInt64, Update: false},
			{Name: "email", Type: FieldTypeString, Update: true},
			{Name: "is_active", Type: FieldTypeBool, Update: false},
		},
		UpdateFieldsMap: make(map[string]Field),
	}

	op.mapFieldsUpdate()

	expectedMap := map[string]Field{
		"name":  {Name: "name", Type: FieldTypeString, Update: true},
		"email": {Name: "email", Type: FieldTypeString, Update: true},
	}

	assert.Equal(t, op.UpdateFieldsMap, expectedMap)
}

//nolint:funlen // тестовая функция
func TestValidateWhereFieldUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      Operation
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			op: Operation{
				WhereFieldsMap: map[string]WhereField{
					"name":      {Field: Field{Name: "name", Type: FieldTypeString, Update: true}, Operator: OperatorEqual, Value: "John Doe"},
					"is_active": {Field: Field{Name: "is_active", Type: FieldTypeBool, Update: false}, Operator: OperatorEqual, Value: true},
					"age":       {Field: Field{Name: "age", Type: FieldTypeInt64, Update: true}, Operator: OperatorEqual, Value: 30},
				},
				UpdateFieldsMap: map[string]Field{
					"email": {Name: "email", Type: FieldTypeString, Update: true},
				},
				Fields: []Field{
					{Name: "name", Type: FieldTypeString, Update: false},
					{Name: "age", Type: FieldTypeInt64, Update: false},
					{Name: "email", Type: FieldTypeString, Update: true},
					{Name: "is_active", Type: FieldTypeBool, Update: false},
				},
				Where: []Where{
					{
						Fields: []WhereField{
							{
								Field:    Field{Name: "name", Type: FieldTypeString, Update: false},
								Operator: OperatorEqual,
								Value:    "John Doe",
							},
							{
								Field:    Field{Name: "is_active", Type: FieldTypeBool, Update: false},
								Operator: OperatorEqual,
								Value:    true,
							},
						},
						Conditions: []Where{
							{
								Fields: []WhereField{
									{
										Field:    Field{Name: "age", Type: FieldTypeInt64, Update: false},
										Operator: OperatorEqual,
									},
									{
										Field:    Field{Name: "is_active", Type: FieldTypeBool, Update: false},
										Operator: OperatorEqual,
										Value:    true,
									},
								},
							},
						},
					},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "error in fields: field not updated and not in where",
			op: Operation{
				WhereFieldsMap: map[string]WhereField{
					"is_active": {Field: Field{Name: "is_active", Type: FieldTypeBool, Update: false}, Operator: OperatorEqual, Value: true},
					"age":       {Field: Field{Name: "age", Type: FieldTypeInt64, Update: true}, Operator: OperatorEqual, Value: 30},
				},
				UpdateFieldsMap: map[string]Field{
					"email": {Name: "email", Type: FieldTypeString, Update: true},
				},
				Fields: []Field{
					{Name: "name", Type: FieldTypeString, Update: false},
					{Name: "age", Type: FieldTypeInt64, Update: false},
					{Name: "email", Type: FieldTypeString, Update: true},
					{Name: "is_active", Type: FieldTypeBool, Update: false},
				},
				Where: []Where{
					{
						Fields: []WhereField{
							{
								Field:    Field{Name: "is_active", Type: FieldTypeBool, Update: false},
								Operator: OperatorEqual,
								Value:    true,
							},
						},
						Conditions: []Where{
							{
								Fields: []WhereField{
									{
										Field:    Field{Name: "age", Type: FieldTypeInt64, Update: false},
										Operator: OperatorEqual,
									},
									{
										Field:    Field{Name: "is_active", Type: FieldTypeBool, Update: false},
										Operator: OperatorEqual,
										Value:    true,
									},
								},
							},
						},
					},
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.op.validateWhereFieldUpdate(tt.op.Where[0])
			tt.wantErr(t, err)
		})
	}
}
