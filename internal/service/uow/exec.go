package uow

import (
	"context"
	"db-worker/internal/storage"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// ExecRequests выполняет запросы к хранилищам.
// В случае неудачи - откатывает коммит, в случае успеха - коммитит транзакцию.
func (s *Service) ExecRequests(ctx context.Context, requests map[storage.Driver]*storage.Request) error {
	tx, err := s.beginTx(ctx, requests)
	if err != nil {
		return fmt.Errorf("error begin transaction: %w", err)
	}

	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	defer func() {
		err := s.finishTx(ctx, tx)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_ID":            tx.ID(),
				"operation":                 s.cfg.Name,
				"service":                   "uow",
				"transaction_requests_num":  len(tx.Requests()),
				"transaction_failed_driver": tx.FailedDriver,
				"transaction_error":         tx.Error(),
				"transaction_status":        tx.Status(),
			}).WithError(err).
				Error("error finishing transaction")
		}
	}()

	logrus.WithFields(logrus.Fields{
		"transaction_ID":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriver,
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("executing requests")

	if err := s.execRequests(ctx, tx); err != nil {
		return fmt.Errorf("error executing requests: %w", err)
	}

	if err := s.Commit(ctx, tx); err != nil {
		return fmt.Errorf("error commit transaction: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"transaction_ID":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriver,
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("all drivers processed requests successfully")

	return nil
}

func (s *Service) execRequests(ctx context.Context, tx storage.TransactionEditor) error {
	for driver, request := range tx.Requests() {
		if err := s.execWithRollback(ctx, tx, driver, func() error {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
			defer cancel()

			return s.execWithTx(timeoutCtx, tx, driver, request)
		}); err != nil {
			return fmt.Errorf("error exec request: %w", err)
		}
	}

	return nil
}

// Commit коммитит транзакцию.
// Транзакция должна быть в статусе in progress.
func (s *Service) Commit(ctx context.Context, tx storage.TransactionEditor) error {
	logrus.WithFields(logrus.Fields{
		"transaction_ID":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriver,
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("committing transaction")

	if !tx.IsInProgress() {
		return fmt.Errorf("transaction status not equal to: %q", storage.TxStatusInProgress)
	}

	for driver := range tx.Requests() {
		if err := s.execWithRollback(ctx, tx, driver, func() error {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
			defer cancel()

			return driver.Commit(timeoutCtx, tx.ID())
		}); err != nil {
			tx.SetFailedStatus(driver.Name(), err)

			return fmt.Errorf("UOW: failed to commit driver %q: %w", driver.Name(), err)
		}
	}

	tx.SetSuccessStatus()

	if err := s.updateTX(ctx, tx); err != nil {
		logrus.WithError(err).Error("error updating transaction when committing")
	}

	return nil
}

// Rollback откатывает транзакцию.
// Транзакция должна быть в статусе failed.
// В случае успеха удаляет транзакцию из map.
func (s *Service) Rollback(ctx context.Context, tx storage.TransactionEditor) error {
	logrus.WithFields(logrus.Fields{
		"transaction_ID":            tx.ID(),
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.Requests()),
		"transaction_failed_driver": tx.FailedDriver,
		"transaction_error":         tx.Error(),
		"transaction_status":        tx.Status(),
	}).Info("rolling back transaction")

	if !tx.IsFailed() {
		return fmt.Errorf("transaction status not equal to: %q", storage.TxStatusFailed)
	}

	for driver := range tx.Requests() {
		// в "сломанном" драйвере мы не можем откатиться, т.к. он не смог выполниться.
		if driver.Name() == tx.FailedDriver() {
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
		defer cancel()

		err := driver.Rollback(ctxTimeout, tx.ID())
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_ID": tx.ID(),
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

	if err := s.finishTx(ctx, tx); err != nil {
		return fmt.Errorf("error finish transaction: %w", err)
	}

	return nil
}

func (s *Service) execWithTx(ctx context.Context, tx storage.TransactionEditor, driver storage.Driver, req *storage.Request) error {
	if err := driver.Exec(ctx, req, tx.ID()); err != nil {
		tx.SetFailedStatus(driver.Name(), err)

		return fmt.Errorf("error exec request: %w", err)
	}

	return nil
}

func (s *Service) execWithRollback(ctx context.Context, tx storage.TransactionEditor, driver storage.Driver, fn func() error) error {
	if err := fn(); err != nil {
		tx.SetFailedStatus(driver.Name(), err)

		rbErr := s.Rollback(ctx, tx)
		if rbErr != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_ID": tx.ID(),
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          rbErr,
			}).Error("failed to rollback transaction")

			return fmt.Errorf("error rollback transaction: %w", rbErr)
		}

		logrus.WithFields(logrus.Fields{
			"transaction_ID": tx.ID(),
			"operation":      s.cfg.Name,
			"service":        "uow",
			"driver":         driver.Name(),
			"error":          err,
		}).Error("failed to exec with rollback")

		return fmt.Errorf("error exec with rollback: %w", err)
	}

	return nil
}
