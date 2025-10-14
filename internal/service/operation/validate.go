package operation

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/service/validator"
	"fmt"
)

func (s *Service) validateMessage(msg map[string]interface{}) error {
	err := s.validateFieldsCount(msg)
	if err != nil {
		return fmt.Errorf("operation: error validate fields: %w", err)
	}

	err = s.validateFieldVals(msg)
	if err != nil {
		return fmt.Errorf("operation: error validate fields values: %w", err)
	}

	return nil
}

func (s *Service) validateFieldsCount(msg map[string]interface{}) error {
	// проверить, что все обязательные поля присутствуют в сообщении
	for _, field := range s.cfg.Fields {
		if field.Required {
			if _, ok := msg[field.Name]; !ok {
				return fmt.Errorf("field %q is required", field.Name)
			}
		}
	}

	return nil
}

func (s *Service) validateFieldVals(msg map[string]interface{}) error {
	for _, field := range s.cfg.Fields {
		val, ok := msg[field.Name]
		if !ok {
			return fmt.Errorf("field %q is not found", field.Name)
		}

		if err := s.validateFieldVal(val, field); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateFieldVal(val any, field operation.Field) error {
	v := validator.New().WithField(field).WithVal(val)

	return v.Validate()
}
