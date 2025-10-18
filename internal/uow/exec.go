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
	tx := s.beginTx(requests)

	for driver, request := range requests {
		if err := s.execWithRollback(ctx, tx, func() error {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
			defer cancel()

			return s.execWithTx(timeoutCtx, tx, driver, request)
		}); err != nil {
			return fmt.Errorf("error exec request: %w", err)
		}
	}

	if err := s.execWithRollback(ctx, tx, func() error {
		return s.Commit(ctx, tx)
	}); err != nil {
		return fmt.Errorf("error commit transaction: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"transaction_id": tx.id,
		"operation":      s.cfg.Name,
		"service":        "uow",
	}).Info("all drivers processed requests successfully")

	return nil
}

// Commit коммитит транзакцию.
// Транзакция должна быть в статусе in progress.
// В случае успеха удаляет транзакцию из map.
func (s *Service) Commit(ctx context.Context, tx *transaction) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id": tx.id,
		"operation":      s.cfg.Name,
		"service":        "uow",
	}).Info("committing transaction")

	if !tx.isInProgress() {
		return fmt.Errorf("transaction status not equal to: %q", txStatusInProgress)
	}

	for driver := range tx.requests {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
		defer cancel()

		err := driver.Commit(timeoutCtx, tx.id)
		if err != nil {
			tx.setFailedDriver(driver.Name())

			tx.setStatus(txStatusFailed)

			return fmt.Errorf("UOW: failed to commit driver %q: %w", driver.Name(), err)
		}
	}

	tx.setStatus(txStatusSuccess)

	if err := s.finishTx(tx); err != nil {
		return fmt.Errorf("error finish transaction: %w", err)
	}

	return nil
}

// Rollback откатывает транзакцию.
// Транзакция должна быть в статусе failed.
// В случае успеха удаляет транзакцию из map.
func (s *Service) Rollback(ctx context.Context, tx *transaction) error {
	logrus.WithFields(logrus.Fields{
		"transaction_id": tx.id,
		"operation":      s.cfg.Name,
		"service":        "uow",
	}).Info("rolling back transaction")

	if !tx.isFailed() {
		return fmt.Errorf("transaction status not equal to: %q", txStatusFailed)
	}

	for _, driver := range s.driversMap {
		// в "сломанном" драйвере мы не можем откатиться, т.к. он не смог выполниться.
		if driver.cfg.Name == tx.failedDriver {
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
		defer cancel()

		err := driver.driver.Rollback(ctxTimeout, tx.id)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.id,
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.cfg.Name,
				"error":          err,
			}).Error("failed to rollback driver")

			continue
			// не выходим из функции, т.к. нужно откатиться у всех драйверов.
		}
	}

	if err := s.finishTx(tx); err != nil {
		return fmt.Errorf("error finish transaction: %w", err)
	}

	return nil
}

func (s *Service) execWithTx(ctx context.Context, tx *transaction, driver storage.Driver, req *storage.Request) error {
	if err := driver.Exec(ctx, req); err != nil {
		tx.setFailedDriver(driver.Name())
		tx.setStatus(txStatusFailed)

		return fmt.Errorf("error exec request: %w", err)
	}

	return nil
}

func (s *Service) execWithRollback(ctx context.Context, tx *transaction, fn func() error) error {
	if err := fn(); err != nil {
		tx.setStatus(txStatusFailed)

		rbErr := s.Rollback(ctx, tx)
		if rbErr != nil {
			return fmt.Errorf("error rollback transaction: %w", rbErr)
		}
		return fmt.Errorf("error exec with rollback: %w", err)
	}

	return nil
}
