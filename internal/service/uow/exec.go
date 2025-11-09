package uow

import (
	"context"
	"db-worker/internal/storage"
	"fmt"

	"github.com/sirupsen/logrus"
)

// ExecRequests выполняет запросы к хранилищам.
// В случае неудачи - откатывает коммит, в случае успеха - коммитит транзакцию.
func (s *Service) ExecRequests(ctx context.Context, requests map[storage.Driver]*storage.Request, raw map[string]any) (err error) {
	tx, err := s.newTx(ctx, requests, raw)
	if err != nil {
		return fmt.Errorf("error creating transaction: %w", err)
	}

	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	defer func() {
		finishErr := s.finishTx(ctx, tx)
		if finishErr != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id":            tx.ID(),
				"operation":                 s.cfg.Name,
				"service":                   "uow",
				"transaction_requests_num":  len(tx.Requests()),
				"transaction_failed_driver": tx.FailedDriverName(),
				"transaction_error":         tx.Error(),
				"transaction_status":        tx.Status(),
			}).WithError(finishErr).
				Error("error finishing transaction")

			// Если основная функция не вернула ошибку, возвращаем ошибку из defer
			if err == nil {
				err = finishErr
			}
		}
	}()

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("executing requests")

	if err := s.execTx(ctx, tx); err != nil {
		return fmt.Errorf("error executing transaction: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("all drivers processed requests successfully")

	return nil
}

// execTx начинает транзакцию в пользовательских хранилищах и выполняет запросы.
// Транзакция должна быть в статусе in progress.
// Если запрос не удалось выполнить, то устанавливает статус failed и возвращает ошибку.
// Если запросы не удалось выполнить, то откатывает транзакцию.
func (s *Service) execTx(ctx context.Context, tx storage.TransactionEditor) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("executing transaction")

	if !tx.IsInProgress() {
		return fmt.Errorf("transaction status not equal to: %q. Real status: %q", storage.TxStatusInProgress, tx.Status())
	}

	// начинаем транзакцию в пользовательских хранилищах
	for driver := range tx.Requests() {
		err := s.beginInDriver(ctx, tx, driver)
		if err != nil {
			return fmt.Errorf("error beginning transaction: %+v", err)
		}
	}

	if err := s.execRequests(ctx, tx); err != nil {
		return fmt.Errorf("error executing requests: %w", err)
	}

	if err := s.Commit(ctx, tx); err != nil {
		return fmt.Errorf("error commit transaction: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("transaction executed successfully")

	return nil
}

func (s *Service) execRequests(ctx context.Context, tx storage.TransactionEditor) error {
	for driver, request := range tx.Requests() {
		if err := s.execWithRollback(ctx, tx, driver, func() error {
			err := s.execWithTx(ctx, tx, driver, request)
			if err != nil {
				return fmt.Errorf("error exec request: %w", err)
			}

			return nil
		}); err != nil {
			return fmt.Errorf("error exec request: %w", err)
		}
	}

	return nil
}

// Commit коммитит транзакцию.
// Транзакция должна быть в статусе in progress.
// Откатывает транзакцию, если не удалось коммитить в одном из драйверов.
func (s *Service) Commit(ctx context.Context, tx storage.TransactionEditor) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("committing transaction")

	if !tx.IsInProgress() {
		return fmt.Errorf("transaction status not equal to: %q. Real status: %q", storage.TxStatusInProgress, tx.Status())
	}

	for driver := range tx.Requests() {
		if err := s.execWithRollback(ctx, tx, driver, func() error {
			err := driver.Commit(ctx, tx.ID())
			if err != nil {
				return fmt.Errorf("error commit driver: %w", err)
			}

			return nil
		}); err != nil {
			return fmt.Errorf("UOW: failed to commit driver %q: %w", driver.Name(), err)
		}
	}

	tx.SetSuccessStatus()

	if err := s.updateTX(ctx, tx.OriginalTx()); err != nil {
		logrus.WithError(err).Error("error updating transaction when committing")

		return fmt.Errorf("error updating transaction when committing: %w", err)
	}

	return nil
}

// Rollback откатывает транзакцию.
// Транзакция должна быть в статусе failed.
func (s *Service) Rollback(ctx context.Context, tx storage.TransactionEditor) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriverName(),
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("rolling back transaction")

	if !tx.IsFailed() {
		return fmt.Errorf("transaction status not equal to: %q. Real status: %q", storage.TxStatusFailed, tx.Status())
	}

	for driver := range tx.Requests() {
		err := driver.Rollback(ctx, tx.ID())
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.ID(),
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          err,
				"requests_num":   len(tx.Requests()),
				"tx_status":      tx.Status(),
			}).Error("failed to rollback driver")

			continue
			// не выходим из функции, т.к. нужно откатиться у всех драйверов.
		}
	}

	return nil
}

// execWithTx выполняет запрос в драйвере. Транзакция должна быть в статусе in progress.
// Если запрос не удалось выполнить, то устанавливает статус failed и возвращает ошибку.
func (s *Service) execWithTx(ctx context.Context, tx storage.TransactionEditor, driver storage.Driver, req *storage.Request) error {
	if !tx.IsInProgress() {
		return fmt.Errorf("transaction status not equal to: %q. Real status: %q", storage.TxStatusInProgress, tx.Status())
	}

	if err := driver.Exec(ctx, req, tx.ID()); err != nil {
		tx.SetFailedStatus(driver, err)

		return fmt.Errorf("error exec request: %w", err)
	}

	return nil
}

// execWithRollback выполняет функцию и откатывает транзакцию, если функция вернула ошибку. Транзакция должна быть в статусе in progress.
func (s *Service) execWithRollback(ctx context.Context, tx storage.TransactionEditor, driver storage.Driver, fn func() error) error {
	if !tx.IsInProgress() {
		return fmt.Errorf("transaction status not equal to: %q. Real status: %q", storage.TxStatusInProgress, tx.Status())
	}

	if err := fn(); err != nil {
		tx.SetFailedStatus(driver, err)

		logrus.WithFields(logrus.Fields{
			"transaction_id": tx.ID(),
			"operation":      s.cfg.Name,
			"service":        "uow",
			"driver":         driver.Name(),
		}).WithError(err).Error("failed to execute requests. Rolling back transaction...")

		rbErr := s.Rollback(ctx, tx)
		if rbErr != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.ID(),
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          rbErr,
			}).Error("failed to rollback transaction")

			return fmt.Errorf("error rollback transaction: %w", rbErr)
		}

		logrus.WithFields(logrus.Fields{
			"transaction_id": tx.ID(),
			"operation":      s.cfg.Name,
			"service":        "uow",
			"driver":         driver.Name(),
			"error":          err,
		}).Error("failed to exec with rollback")

		return fmt.Errorf("error exec with rollback: %w", err)
	}

	return nil
}
