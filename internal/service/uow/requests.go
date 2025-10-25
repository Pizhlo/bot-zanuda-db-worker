package uow

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"encoding/json"
	"fmt"
)

// saveRequests сохраняет запросы, принадлежащие транзакции.
// requests - запросы оригинальной транзакции.
func (s *Service) saveRequests(ctx context.Context, utilityTx storage.TransactionEditor, requests map[storage.Driver]*storage.Request) error {
	op := s.operationForSavingRequests(utilityTx)

	// составляем запросы для сохранения пользовательских запросов, принадлежащих транзакции
	for driver, req := range requests {
		msg, err := fieldsForReq(*req, utilityTx.ID(), string(driver.Type()), driver.Name())
		if err != nil {
			return fmt.Errorf("error building fields for request: %w", err)
		}

		// создаем запросы для сохранения транзакции
		reqs, err := s.BuildRequests(msg, s.requestsDriversMap, op)
		if err != nil {
			return fmt.Errorf("error building requests for saving requests: %w", err)
		}

		utilityTx.SaveRequests(reqs)

		err = s.execRequests(ctx, utilityTx)
		if err != nil {
			return fmt.Errorf("error while saving requests: %w", err)
		}
	}

	return nil
}

// operationForSavingRequests составляет операцию для сохранения пользовательских запросов.
func (s *Service) operationForSavingRequests(tx storage.TransactionEditor) operation.Operation {
	return operation.Operation{
		Name:    fmt.Sprintf("system operation for saving requests for tx %s", tx.ID()),
		Type:    operation.OperationTypeCreate,
		Timeout: s.cfg.Timeout,
	}
}

// fieldsForReq составляет поля для составления запросов для сохранения \ изменения пользовательских запросов.
func fieldsForReq(req storage.Request, txID string, driverType, driverName string) (map[string]any, error) {
	jsonData, err := json.Marshal(req.Raw)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request raw: %w", err)
	}

	return map[string]any{
		"data":        jsonData,
		"tx_id":       txID,
		"driver_type": driverType,
		"driver_name": driverName,
	}, nil
}
