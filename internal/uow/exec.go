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

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.id,
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.requests),
		"transaction_failed_driver": tx.failedDriver,
		"transaction_error":         tx.err,
		"transaction_status":        tx.status,
	}).Info("executing requests")

	for driver, request := range requests {
		if err := s.execWithRollback(ctx, tx, driver, func() error {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
			defer cancel()

			return s.execWithTx(timeoutCtx, tx, driver, request)
		}); err != nil {
			return fmt.Errorf("error exec request: %w", err)
		}
	}

	if err := s.Commit(ctx, tx); err != nil {
		return fmt.Errorf("error commit transaction: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.id,
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.requests),
		"transaction_failed_driver": tx.failedDriver,
		"transaction_error":         tx.err,
		"transaction_status":        tx.status,
	}).Info("all drivers processed requests successfully")

	return nil
}

// Commit коммитит транзакцию.
// Транзакция должна быть в статусе in progress.
// В случае успеха удаляет транзакцию из map.
func (s *Service) Commit(ctx context.Context, tx *transaction) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.id,
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.requests),
		"transaction_failed_driver": tx.failedDriver,
		"transaction_error":         tx.err,
		"transaction_status":        tx.status,
	}).Info("committing transaction")

	if !tx.isInProgress() {
		return fmt.Errorf("transaction status not equal to: %q", txStatusInProgress)
	}

	for driver := range tx.requests {
		if err := s.execWithRollback(ctx, tx, driver, func() error {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
			defer cancel()

			return driver.Commit(timeoutCtx, tx.id)
		}); err != nil {
			tx.setFailedStatus(driver.Name(), err)

			return fmt.Errorf("UOW: failed to commit driver %q: %w", driver.Name(), err)
		}
	}

	tx.setSuccessStatus()

	if err := s.finishTx(ctx, tx); err != nil {
		return fmt.Errorf("error finish transaction: %w", err)
	}

	return nil
}

// Rollback откатывает транзакцию.
// Транзакция должна быть в статусе failed.
// В случае успеха удаляет транзакцию из map.
func (s *Service) Rollback(ctx context.Context, tx *transaction) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.id,
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.requests),
		"transaction_failed_driver": tx.failedDriver,
		"transaction_error":         tx.err,
		"transaction_status":        tx.status,
	}).Info("rolling back transaction")

	if !tx.isFailed() {
		return fmt.Errorf("transaction status not equal to: %q", txStatusFailed)
	}

	for driver := range tx.requests {
		// в "сломанном" драйвере мы не можем откатиться, т.к. он не смог выполниться.
		if driver.Name() == tx.failedDriver {
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
		defer cancel()

		err := driver.Rollback(ctxTimeout, tx.id)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.id,
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          err,
				"requests_num":   len(tx.requests),
				"tx_status":      tx.status,
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

func (s *Service) execWithTx(ctx context.Context, tx *transaction, driver storage.Driver, req *storage.Request) error {
	if err := driver.Exec(ctx, req, tx.id); err != nil {
		tx.setFailedStatus(driver.Name(), err)

		return fmt.Errorf("error exec request: %w", err)
	}

	return nil
}

func (s *Service) execWithRollback(ctx context.Context, tx *transaction, driver storage.Driver, fn func() error) error {
	if err := fn(); err != nil {
		tx.setFailedStatus(driver.Name(), err)

		rbErr := s.Rollback(ctx, tx)
		if rbErr != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.id,
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          rbErr,
			}).Error("failed to rollback transaction")

			return fmt.Errorf("error rollback transaction: %w", rbErr)
		}

		logrus.WithFields(logrus.Fields{
			"transaction_id": tx.id,
			"operation":      s.cfg.Name,
			"service":        "uow",
			"driver":         driver.Name(),
			"error":          err,
		}).Error("failed to exec with rollback")

		return fmt.Errorf("error exec with rollback: %w", err)
	}

	return nil
}
