package uow

import (
	"context"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

// beginTx начинает транзакцию.
func (s *Service) beginTx(ctx context.Context, requests map[storage.Driver]*storage.Request) (*storage.Transaction, error) {
	tx, err := s.newTx(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction: %w", err)
	}

	// начинаем транзакцию в пользовательских хранилищах
	for driver := range requests {
		err := s.beginInDriver(ctx, tx, driver)
		if err != nil {
			return nil, fmt.Errorf("error beginning transaction: %+v", err)
		}
	}

	return tx, nil
}

// newTx создает новую транзакцию и сохраняет в системное хранилище.
func (s *Service) newTx(ctx context.Context, requests map[storage.Driver]*storage.Request) (*storage.Transaction, error) {
	tx, err := storage.NewTransaction(requests, s.instanceID, s.cfg.Hash)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"transaction_id":           tx.ID(),
		"operation":                s.cfg.Name,
		"service":                  "uow",
		"transaction_requests_num": len(requests),
	}).Info("creating new transaction")

	logrus.WithFields(logrus.Fields{
		"transaction_id":           tx.ID(),
		"operation":                s.cfg.Name,
		"service":                  "uow",
		"transaction_requests_num": len(requests),
	}).Info("saving new transaction")

	err = s.saveTx(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("error saving transaction: %w", err)
	}

	return tx, nil
}

func (s *Service) beginInDriver(ctx context.Context, tx storage.TransactionEditor, driver storage.Driver) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id": tx.ID(),
		"operation":      s.cfg.Name,
		"service":        "uow",
		"driver":         driver.Name(),
	}).Info("beginning transaction in driver")

	beginErr := driver.Begin(ctx, tx.ID())
	if beginErr != nil {
		logrus.WithFields(logrus.Fields{
			"transaction_id": tx.ID(),
			"operation":      s.cfg.Name,
			"service":        "uow",
			"driver":         driver.Name(),
			"error":          beginErr,
		}).Error("failed to begin transaction in driver")

		tx.OriginalTx().SetFailedStatus(driver, beginErr)

		// обновляем только если транзакция пользовательская
		if tx.OriginalTx() != tx {
			return fmt.Errorf("error beginning transaction in driver %q: %w", driver.Name(), beginErr)
		}

		updateErr := s.updateTX(ctx, tx.OriginalTx())
		if updateErr != nil {
			return fmt.Errorf("error updating transaction: %w", updateErr)
		}

		// если не удалось начать транзакцию в одном из драйверов, то завершаем транзакцию.

		finishErr := s.finishTx(ctx, tx.OriginalTx())
		if finishErr != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.ID(),
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          finishErr,
			}).Error("failed to finish transaction in driver after failed to begin transaction")

			return fmt.Errorf("failed to begin transaction in driver %q: %w (also failed to finish: %v)", driver.Name(), beginErr, finishErr)
		}

		return fmt.Errorf("failed to begin transaction in driver %q: %w", driver.Name(), beginErr)
	}

	tx.AddBegunDriver(driver)

	return nil
}

// finishTx завершает транзакцию, если по каким-то причинам не удалось начать транзакцию в одном из драйверов.
// Транзакция не должна быть в статусе in progress.
// В случае успеха удаляет транзакцию из map.
func (s *Service) finishTx(ctx context.Context, tx storage.TransactionEditor) error {
	// либо успешная, либо неудачная транзакция
	if tx.IsInProgress() {
		return fmt.Errorf("transaction status equal to: %q, but expected: %q or %q", storage.TxStatusInProgress, storage.TxStatusSuccess, storage.TxStatusFailed)
	}

	var errs []error

	for driver := range tx.Begun() {
		err := driver.FinishTx(ctx, tx.ID())
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.ID(),
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          err,
			}).Error("failed to finish transaction in driver")

			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to finish transaction in drivers: %w", errors.Join(errs...))
	}

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("transaction finished")

	return nil
}

// saveTx сохраняет новую транзакцию в кэш и хранилище.
// Для сохранения уже имеющейся транзакции необходимо использовать метод updateTx.
func (s *Service) saveTx(ctx context.Context, tx storage.TransactionEditor) error {
	// создаем вспомогательную транзакцию для сохранения основной.
	// вспомогательная транзакция нужна только как прослойка для сохранения основной, и не будет сохранена в БД.
	if tx == nil {
		return fmt.Errorf("error creating utility transaction: %w", errors.New("original transaction not provided"))
	}

	origTx, ok := tx.OriginalTx().(*storage.Transaction)
	if !ok || origTx == nil {
		return fmt.Errorf("error creating utility transaction: %w", errors.New("original transaction not provided"))
	}

	utilityTx, err := storage.NewUtilityTransaction(
		storage.WithDriver(s.storage),
		storage.WithOriginalTx(origTx),
	)
	if err != nil {
		return fmt.Errorf("error creating utility transaction: %w", err)
	}

	msg := s.fieldsForTx(tx)

	op := s.operationForSavingTx(tx)

	// создаем запросы для сохранения транзакции
	reqs, err := s.BuildRequests(msg, s.transactionDriversMap, op)
	if err != nil {
		return fmt.Errorf("error building requests for saving transaction: %w", err)
	}

	utilityTx.SaveRequests(reqs)

	// начинаем транзакцию в хранилищах
	for driver := range reqs {
		err := s.beginInDriver(ctx, utilityTx, driver)
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}
	}

	err = s.execRequests(ctx, utilityTx)
	if err != nil {
		return fmt.Errorf("error while executing requests: %w", err)
	}

	err = s.saveRequests(ctx, utilityTx, tx.Requests())
	if err != nil {
		return fmt.Errorf("error while saving requests: %w", err)
	}

	// сохранили транзакцию
	for driver := range reqs {
		err = s.execWithRollback(ctx, utilityTx, driver, func() error {
			return driver.Commit(ctx, utilityTx.ID())
		})
		if err != nil {
			return fmt.Errorf("error while committing transaction: %w", err)
		}
	}

	return nil
}

// updateTX обновляет транзакцию в кэше и хранилище.
func (s *Service) updateTX(ctx context.Context, tx storage.TransactionEditor) error {
	origTx, ok := tx.(*storage.Transaction)
	if !ok {
		return fmt.Errorf("transaction is not original")
	}

	// создаем вспомогательную транзакцию для сохранения основной.
	// вспомогательная транзакция нужна только как прослойка для сохранения основной, и не будет сохранена в БД.
	utilityTx, err := storage.NewUtilityTransaction(
		storage.WithDriver(s.storage),
		storage.WithOriginalTx(origTx),
	)
	if err != nil {
		return fmt.Errorf("error creating utility transaction: %w", err)
	}

	msg := s.fieldsForTx(tx)

	op := s.operationForUpdatingTx()

	// создаем запросы для сохранения транзакции
	reqs, err := s.BuildRequests(msg, s.transactionDriversMap, op)
	if err != nil {
		return fmt.Errorf("error building requests for updating transaction: %w", err)
	}

	utilityTx.SaveRequests(reqs)

	// начинаем транзакцию в хранилищах
	for driver := range reqs {
		err := s.beginInDriver(ctx, utilityTx, driver)
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}
	}

	err = s.execRequests(ctx, utilityTx)
	if err != nil {
		return fmt.Errorf("error while updating transaction: %w", err)
	}

	// коммитим инфу про транзакции
	for driver := range reqs {
		err = s.execWithRollback(ctx, utilityTx, driver, func() error {
			return driver.Commit(ctx, utilityTx.ID())
		})
		if err != nil {
			return fmt.Errorf("error while committing transaction: %w", err)
		}
	}

	return nil
}

// fieldsForTx составляет поля для составления запросов для сохранения \ изменения транзакции.
func (s *Service) fieldsForTx(tx storage.TransactionEditor) map[string]any {
	return map[string]interface{}{
		"id":             tx.ID(),
		"status":         tx.Status(),
		"error":          tx.ErrorString(),
		"instance_id":    s.instanceID,
		"failed_driver":  tx.FailedDriverName(),
		"operation_hash": s.cfg.Hash,
	}
}

// operationForSavingTx составляет операцию для сохранения транзакции.
func (s *Service) operationForSavingTx(tx storage.TransactionEditor) operation.Operation {
	return operation.Operation{
		Name:    fmt.Sprintf("system operation for saving tx %s", tx.ID()),
		Type:    operation.OperationTypeCreate,
		Timeout: s.cfg.Timeout,
	}
}

// operationForUpdatingTx составляет операцию для обновления транзакции.
func (s *Service) operationForUpdatingTx() operation.Operation {
	return operation.Operation{
		Name:    "system operation for updating tx",
		Type:    operation.OperationTypeUpdate,
		Timeout: s.cfg.Timeout,
		Where: []operation.Where{
			{
				Fields: []operation.WhereField{
					{
						Field: operation.Field{
							Name: "id",
						},
						Operator: operation.OperatorEqual,
					},
				},
			},
		},
		WhereFieldsMap: map[string]operation.WhereField{
			"id": {
				Field: operation.Field{
					Name: "id",
				},
				Operator: operation.OperatorEqual,
			},
		},
		UpdateFieldsMap: map[string]operation.Field{
			"status": {
				Name: "status",
			},
			"error": {
				Name: "error",
			},
			"instance_id": {
				Name: "instance_id",
			},
			"failed_driver": {
				Name: "failed_driver",
			},
		},
	}
}
